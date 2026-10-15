//go:build integration

// Package integration contains tests that prove transactional atomicity and event relay
// behaviour against a real PostgreSQL instance.
//
// Unit tests cannot demonstrate atomicity. Rollback behaviour only exists in the
// database's transaction log, so these tests deliberately use a live database rather than
// an in-memory fake, which would happily report success for a write that was never
// durable.
//
// IMPORTANT: do NOT run a real `outbox-relayer` against the same database while these
// tests execute. The relayer would drain the outbox and mark rows PUBLISHED before the
// fake publisher in this package ever sees them, and most tests here would fail for reasons
// that have nothing to do with the code under test. If relay tests fail unexpectedly, check
// for a stray `make relayer` first.
//
// Run with:
//
//	make up migrate
//	make integration
//
// Set TEST_DATABASE_URL to point at a different instance. The schema must already contain
// the tables from services/payments-api/migrations.
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	kafkapkg "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/kafka"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/outbox"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/payments"
)

// fakePublisher records what the relayer tried to publish and can be made to fail on
// demand, so DLQ routing and retry behaviour are exercised without waiting for a real
// broker to misbehave.
type fakePublisher struct {
	mu sync.Mutex

	// published records every successful topic the relayer wrote to.
	published []publishedMessage

	// failTopics maps a topic to the error every publish to it returns.
	failTopics map[string]error

	// failTimes, when non-nil, is decremented per publish to that topic; a publish
	// succeeds once it reaches zero. Used to model an outage that heals.
	failTimes map[string]int

	enabled bool
	closed  bool
}

type publishedMessage struct {
	topic   string
	key     string
	payload []byte
	headers map[string]string
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{enabled: true}
}

func (f *fakePublisher) Publish(_ context.Context, topic, key string, value []byte, headers map[string]string) (kafkapkg.PublishResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if remaining, ok := f.failTimes[topic]; ok && remaining > 0 {
		f.failTimes[topic] = remaining - 1
		return kafkapkg.PublishResult{}, &kafkapkg.RetriableError{Err: fmt.Errorf("simulated outage on %s", topic)}
	}
	if err, ok := f.failTopics[topic]; ok && err != nil {
		return kafkapkg.PublishResult{}, err
	}

	copied := make(map[string]string, len(headers))
	for k, v := range headers {
		copied[k] = v
	}
	f.published = append(f.published, publishedMessage{topic: topic, key: key, payload: value, headers: copied})
	return kafkapkg.PublishResult{Partition: 2, Offset: 42}, nil
}

func (f *fakePublisher) Enabled() bool { return f.enabled }

func (f *fakePublisher) Flush(time.Duration) int { return 0 }

func (f *fakePublisher) Close() { f.closed = true }

func (f *fakePublisher) messagesFor(topic string) []publishedMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []publishedMessage
	for _, m := range f.published {
		if m.topic == topic {
			out = append(out, m)
		}
	}
	return out
}

func (f *fakePublisher) failTopic(topic string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTopics == nil {
		f.failTopics = map[string]error{}
	}
	f.failTopics[topic] = err
}

func (f *fakePublisher) clearFailures() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failTopics = nil
	f.failTimes = nil
}

// newRelayerHarness wires a payments Service, an outbox Store, and a Relayer against the
// live database used by the other tests in this package.
func newRelayerHarness(t *testing.T) (*payments.Service, *pgxpool.Pool, *outbox.Relayer, *outbox.Store, *fakePublisher) {
	t.Helper()

	svc, pool := setup(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	publisher := newFakePublisher()
	store := outbox.NewStore(pool)

	cfg := outbox.DefaultRelayerConfig()
	cfg.BatchSize = 50
	cfg.PollInterval = 20 * time.Millisecond
	cfg.Lease = 5 * time.Second
	cfg.MaxAttempts = 3
	cfg.Backoff = outbox.BackoffPolicy{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond, JitterFraction: 0}

	relayer := outbox.NewRelayer(store, publisher, cfg, logger)

	t.Cleanup(func() { truncate(t, pool) })

	return svc, pool, relayer, store, publisher
}

// runRelayer starts the relayer and returns a stop function that cancels it and waits for
// a clean exit.
//
// The returned function is safe to call more than once: tests commonly stop the relayers
// mid-assertion and again on cleanup, and a second call must not block forever waiting on a
// result channel the first call already drained.
func runRelayer(t *testing.T, relayer *outbox.Relayer) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relayer.Run(ctx) }()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("relayer returned an error on shutdown: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Error("relayer did not stop within 20s")
			}
		})
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func outboxStatusCount(t *testing.T, pool *pgxpool.Pool, status string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE status = $1`, status).Scan(&n); err != nil {
		t.Fatalf("count outbox rows with status %s: %v", status, err)
	}
	return n
}

// The core relay guarantee: a committed payment reaches Kafka and its row is marked
// PUBLISHED.
func TestRelayerPublishesCommittedPayment(t *testing.T) {
	svc, pool, relayer, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	key := "relay-" + uuid.New().String()
	resp, err := svc.CreatePayment(ctx, newRequest(t, key, 5000))
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	// Nothing has been relayed yet.
	if got := len(publisher.messagesFor("payments.events.v1")); got != 0 {
		t.Fatalf("publisher received %d messages before the relayer started", got)
	}

	stop := runRelayer(t, relayer)
	defer stop()

	waitFor(t, 15*time.Second, "the event to be published", func() bool {
		return len(publisher.messagesFor("payments.events.v1")) == 1
	})
	waitFor(t, 15*time.Second, "the row to be marked PUBLISHED", func() bool {
		return outboxStatusCount(t, pool, "PUBLISHED") == 1
	})

	msg := publisher.messagesFor("payments.events.v1")[0]

	// Keying by aggregate id keeps every event for one payment on one partition.
	if msg.key != resp.GetPaymentId() {
		t.Errorf("partition key = %q, want the aggregate id %q", msg.key, resp.GetPaymentId())
	}

	event, err := outbox.Unmarshal(msg.payload)
	if err != nil {
		t.Fatalf("published payload is not a valid events.v1 envelope: %v\n%s", err, msg.payload)
	}
	if event.GetPaymentCreated().GetPaymentId() != resp.GetPaymentId() {
		t.Errorf("published payment_id = %q, want %q", event.GetPaymentCreated().GetPaymentId(), resp.GetPaymentId())
	}
	if event.GetCorrelationId() != key {
		t.Errorf("correlation_id = %q, want the idempotency key %q", event.GetCorrelationId(), key)
	}

	// Operators and DLQ tooling read these headers; consumers use the envelope.
	if msg.headers["x-event-type"] != "payment.created" {
		t.Errorf("x-event-type header = %q, want payment.created", msg.headers["x-event-type"])
	}
	if msg.headers["x-aggregate-id"] != resp.GetPaymentId() {
		t.Errorf("x-aggregate-id header = %q, want %q", msg.headers["x-aggregate-id"], resp.GetPaymentId())
	}
}

// A transient broker failure must leave the row PENDING and be retried, not dead-lettered.
func TestRelayerRetriesTransientPublishFailure(t *testing.T) {
	svc, pool, relayer, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	publisher.failTopic("payments.events.v1", &kafkapkg.RetriableError{Err: errors.New("broker down")})

	if _, err := svc.CreatePayment(ctx, newRequest(t, "retry-"+uuid.New().String(), 5000)); err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	// Drain the first cycle, then let the broker recover.
	stopOnce := runRelayer(t, relayer)
	waitFor(t, 15*time.Second, "the first attempt to be recorded", func() bool {
		return storeAttempts(t, pool, "retry") >= 1
	})
	stopOnce()

	// The row must still be PENDING, never FAILED: a broker outage is not a poison message.
	if got := outboxStatusCount(t, pool, "FAILED"); got != 0 {
		t.Errorf("FAILED rows = %d, want 0; a transient failure must not be dead-lettered", got)
	}

	publisher.clearFailures()
	restart := runRelayer(t, relayer)
	defer restart()

	waitFor(t, 20*time.Second, "the event to publish after recovery", func() bool {
		return outboxStatusCount(t, pool, "PUBLISHED") == 1
	})
	if got := len(publisher.messagesFor("payments.events.v1")); got != 1 {
		t.Errorf("published messages = %d, want exactly 1", got)
	}
}

// A permanent failure must be routed to the dead-letter topic and the row marked FAILED.
// Getting this wrong in the other direction retries forever and blocks the outbox.
func TestRelayerDeadLettersPermanentFailure(t *testing.T) {
	svc, pool, relayer, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	publisher.failTopic("payments.events.v1", &kafkapkg.TerminalError{Err: errors.New("message too large")})

	if _, err := svc.CreatePayment(ctx, newRequest(t, "dlq-"+uuid.New().String(), 5000)); err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	stop := runRelayer(t, relayer)
	defer stop()

	waitFor(t, 15*time.Second, "the row to be marked FAILED", func() bool {
		return outboxStatusCount(t, pool, "FAILED") == 1
	})

	dlq := publisher.messagesFor("payments.events.v1.dlq")
	if len(dlq) != 1 {
		t.Fatalf("DLQ messages = %d, want exactly 1", len(dlq))
	}

	msg := dlq[0]
	if msg.headers["x-dead-letter-reason"] != "terminal publish failure" {
		t.Errorf("x-dead-letter-reason = %q", msg.headers["x-dead-letter-reason"])
	}
	if msg.headers["x-dead-letter-error"] == "" {
		t.Error("x-dead-letter-error must carry the cause for triage")
	}

	// The original payload must survive so the event can be replayed after triage.
	event, err := outbox.Unmarshal(msg.payload)
	if err != nil {
		t.Fatalf("DLQ payload is not a valid envelope: %v", err)
	}
	if event.GetEventType() != "payment.created" {
		t.Errorf("DLQ payload event_type = %q", event.GetEventType())
	}

	// Nothing may reach the live topic.
	if got := len(publisher.messagesFor("payments.events.v1")); got != 0 {
		t.Errorf("published %d messages to the live topic; a terminal failure must not deliver", got)
	}
}

// When attempts are exhausted the event is dead-lettered even though the failure looked
// transient, so a permanently broken broker cannot grow the outbox without bound.
func TestRelayerDeadLettersAfterMaxAttempts(t *testing.T) {
	svc, pool, relayer, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	// MaxAttempts is 3 in the harness.
	publisher.failTopic("payments.events.v1", &kafkapkg.RetriableError{Err: errors.New("broker down")})

	if _, err := svc.CreatePayment(ctx, newRequest(t, "exhaust-"+uuid.New().String(), 5000)); err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	stop := runRelayer(t, relayer)
	defer stop()

	waitFor(t, 25*time.Second, "the row to be dead-lettered after exhausting retries", func() bool {
		return outboxStatusCount(t, pool, "FAILED") == 1
	})

	if got := len(publisher.messagesFor("payments.events.v1.dlq")); got != 1 {
		t.Errorf("DLQ messages = %d, want exactly 1", got)
	}
	if got := storeAttempts(t, pool, "exhaust"); got < 3 {
		t.Errorf("attempts recorded = %d, want at least 3", got)
	}
}

// If the dead-letter write itself fails, the row must stay PENDING. Marking it FAILED would
// drop the event with no copy anywhere, which is the one outcome the outbox forbids.
func TestRelayerDoesNotDropEventWhenDLQWriteFails(t *testing.T) {
	svc, pool, relayer, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	publisher.failTopic("payments.events.v1", &kafkapkg.TerminalError{Err: errors.New("message too large")})
	publisher.failTopic("payments.events.v1.dlq", &kafkapkg.RetriableError{Err: errors.New("dlq unavailable")})

	if _, err := svc.CreatePayment(ctx, newRequest(t, "dlqfail-"+uuid.New().String(), 5000)); err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	stop := runRelayer(t, relayer)
	defer stop()

	waitFor(t, 15*time.Second, "the DLQ write to be attempted", func() bool {
		return outboxStatusCount(t, pool, "PENDING") == 1 && storeAttempts(t, pool, "dlqfail") >= 1
	})

	time.Sleep(500 * time.Millisecond)
	if got := outboxStatusCount(t, pool, "FAILED"); got != 0 {
		t.Errorf("FAILED rows = %d, want 0; an event must never be dropped because its "+
			"dead-letter write failed", got)
	}
}

// Two relayer replicas must not publish the same event twice. SKIP LOCKED plus the lease is
// what makes concurrent polling safe.
func TestConcurrentRelayersDoNotDuplicate(t *testing.T) {
	svc, pool, _, _, publisher := newRelayerHarness(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const paymentsToCreate = 12
	keys := make([]string, 0, paymentsToCreate)
	for i := 0; i < paymentsToCreate; i++ {
		key := fmt.Sprintf("concurrent-%d-%s", i, uuid.New().String())
		if _, err := svc.CreatePayment(ctx, newRequest(t, key, int64(1000+i))); err != nil {
			t.Fatalf("CreatePayment %d: %v", i, err)
		}
		keys = append(keys, key)
	}

	store := outbox.NewStore(pool)
	cfg := outbox.DefaultRelayerConfig()
	cfg.BatchSize = 4
	cfg.PollInterval = 5 * time.Millisecond
	cfg.Lease = 2 * time.Second
	cfg.Backoff = outbox.BackoffPolicy{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond}

	// Three replicas polling the same table.
	stoppers := make([]func(), 3)
	for i := 0; i < 3; i++ {
		relayer := outbox.NewRelayer(store, publisher, cfg, logger)
		stoppers[i] = runRelayer(t, relayer)
	}
	defer func() {
		for _, s := range stoppers {
			s()
		}
	}()

	waitFor(t, 40*time.Second, "every event to be published exactly once", func() bool {
		return outboxStatusCount(t, pool, "PUBLISHED") == paymentsToCreate
	})

	// Stop the replicas before counting, so an in-flight publish cannot race the check.
	for _, s := range stoppers {
		s()
	}

	published := publisher.messagesFor("payments.events.v1")
	if len(published) != paymentsToCreate {
		ids := map[string]int{}
		for _, m := range published {
			ids[m.key]++
		}
		var dupes []string
		for id, n := range ids {
			if n > 1 {
				dupes = append(dupes, fmt.Sprintf("%s x%d", id, n))
			}
		}
		t.Errorf("published %d messages for %d payments (duplicates: %v); SKIP LOCKED plus "+
			"the lease must prevent a row being claimed twice", len(published), paymentsToCreate, dupes)
	}

	_ = keys
}

// Payments must keep working with the relayer not running at all. The outbox exists so a
// missing or stalled relayer degrades delivery, never ingestion.
func TestPaymentsSucceedWhileRelayerIsDown(t *testing.T) {
	svc, pool, _, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	// No relayer is started.
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := svc.CreatePayment(ctx, newRequest(t, fmt.Sprintf("norelay-%d-%s", i, uuid.New().String()), 1000)); err != nil {
			t.Fatalf("CreatePayment %d failed with no relayer running: %v", i, err)
		}
	}

	if got := outboxStatusCount(t, pool, "PENDING"); got != n {
		t.Errorf("PENDING rows = %d, want %d; events must accumulate rather than be lost", got, n)
	}
	if got := len(publisher.messagesFor("payments.events.v1")); got != 0 {
		t.Errorf("published %d messages; nothing should be published without a relayer", got)
	}

	// The backlog must be intact and drainable.
	depth, err := outbox.NewStore(pool).PendingDepth(ctx)
	if err != nil {
		t.Fatalf("PendingDepth: %v", err)
	}
	if depth != n {
		t.Errorf("PendingDepth = %d, want %d", depth, n)
	}
}

// A relayer that stops cleanly mid-backlog must leave every event recoverable.
func TestRelayerShutdownPreservesBacklog(t *testing.T) {
	svc, pool, relayer, _, publisher := newRelayerHarness(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := svc.CreatePayment(ctx, newRequest(t, fmt.Sprintf("stop-%d-%s", i, uuid.New().String()), 1000)); err != nil {
			t.Fatalf("CreatePayment %d: %v", i, err)
		}
	}

	stop := runRelayer(t, relayer)
	stop()

	// Either everything published, or nothing was lost: the two together are the invariant.
	published := len(publisher.messagesFor("payments.events.v1"))
	pending := outboxStatusCount(t, pool, "PENDING")
	if published+pending != 5 {
		t.Errorf("published(%d) + pending(%d) = %d, want 5; shutdown must lose no event",
			published, pending, published+pending)
	}
}

// storeAttempts returns the highest recorded attempt count for rows matching a key prefix.
func storeAttempts(t *testing.T, pool *pgxpool.Pool, keyPrefix string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var n int
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(o.attempts), 0)
		FROM outbox o
		JOIN payments p ON p.id::text = o.aggregate_id
		WHERE p.idempotency_key LIKE $1`, keyPrefix+"%").Scan(&n)
	if err != nil {
		t.Fatalf("read attempts for %s: %v", keyPrefix, err)
	}
	return n
}
