//go:build integration

// Package integration contains tests that prove transactional atomicity against a real
// PostgreSQL instance.
//
// Unit tests cannot demonstrate atomicity. Rollback behaviour only exists in the
// database's transaction log, so these tests deliberately use a live database rather
// than an in-memory fake, which would happily report success for a write that was never
// durable.
//
// Run with:
//
//	make up
//	make integration
//
// Set TEST_DATABASE_URL to point at a different instance. The schema must already
// contain the tables from deploy/local/migrations.
package integration

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentsv1 "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/gen/payments/v1"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/config"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/outbox"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/payments"
)

const defaultTestDatabaseURL = "postgres://resilient:resilient@localhost:5432/payments?sslmode=disable"

// discardRedisLogger silences go-redis's internal logger. The tests deliberately run
// against a dead Redis port, which would otherwise flood the output with dial errors
// that are the expected condition rather than a failure.
type discardRedisLogger struct{}

func (discardRedisLogger) Printf(_ context.Context, _ string, _ ...interface{}) {}

// setup builds a Service backed by a live database.
//
// Redis is deliberately pointed at a closed port. Every request therefore skips the
// idempotency fast path and is forced through the PostgreSQL path, which is the only
// path these tests are about. It also exercises the documented guarantee that a
// dead cache must never block or fail a payment.
func setup(t *testing.T) (*payments.Service, *pgxpool.Pool) {
	t.Helper()

	redis.SetLogger(discardRedisLogger{})

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultTestDatabaseURL
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		t.Fatalf("test database unreachable at %s: %v\n"+
			"start it with `make up`, or set TEST_DATABASE_URL", databaseURL, err)
	}

	// Fail loudly rather than with a confusing "relation does not exist" later on.
	for _, table := range []string{"payments", "outbox"} {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)`,
			table).Scan(&exists); err != nil {
			pool.Close()
			t.Fatalf("schema check failed: %v", err)
		}
		if !exists {
			pool.Close()
			t.Fatalf("table %q does not exist; apply deploy/local/migrations (make reset && make up)", table)
		}
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1", // intentionally closed
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
		MaxRetries:   0,
	})
	t.Cleanup(func() { _ = redisClient.Close() })

	cfg := config.Load()
	cfg.DatabaseURL = databaseURL
	cfg.DBTimeout = 10 * time.Second

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := payments.NewService(pool, redisClient, cfg, logger)

	t.Cleanup(func() {
		truncate(t, pool)
		pool.Close()
	})

	return svc, pool
}

// truncate isolates each test so ordering never masks a leaked row.
func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `TRUNCATE outbox, payments`); err != nil {
		t.Logf("cleanup: truncate failed: %v", err)
	}
}

func newRequest(t *testing.T, idemKey string, amountCents int64) *paymentsv1.CreatePaymentRequest {
	t.Helper()
	return &paymentsv1.CreatePaymentRequest{
		IdempotencyKey: idemKey,
		FromAccount:   "acc-alice",
		ToAccount:     "acc-bob",
		AmountCents:   amountCents,
		Currency:      "EUR",
	}
}

func countRows(t *testing.T, pool *pgxpool.Pool, table, idemKey string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var n int
	var err error
	if table == "payments" {
		err = pool.QueryRow(ctx, `SELECT count(*) FROM payments WHERE idempotency_key=$1`, idemKey).Scan(&n)
	} else {
		// outbox.aggregate_id is TEXT by design so the relayer can stage events for
		// aggregates that are not yet UUID-backed; cast the payment id to match.
		err = pool.QueryRow(ctx, `SELECT count(*) FROM outbox o JOIN payments p ON p.id::text = o.aggregate_id WHERE p.idempotency_key=$1`, idemKey).Scan(&n)
	}
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// A newly created payment must be accompanied by exactly one staged event.
func TestNewPaymentCommitsPaymentAndOutboxRow(t *testing.T) {
	svc, pool := setup(t)
	ctx := context.Background()
	key := "it-new-" + uuid.New().String()

	resp, err := svc.CreatePayment(ctx, newRequest(t, key, 5000))
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	if resp.GetPaymentId() == "" {
		t.Fatal("CreatePayment returned an empty payment_id")
	}
	if got := countRows(t, pool, "payments", key); got != 1 {
		t.Errorf("payments rows = %d, want 1", got)
	}
	if got := countRows(t, pool, "outbox", key); got != 1 {
		t.Errorf("outbox rows = %d, want 1", got)
	}
}

// THE CENTRAL TEST OF THE OUTBOX PATTERN.
//
// A payment and its event must be all-or-nothing. If the event cannot be staged, the
// payment must not exist either, because a committed payment with no event is a
// transfer no downstream system ever learns about.
//
// The failure is injected with a database trigger rather than a test hook, so the
// production code path is exercised exactly as it runs in production.
func TestOutboxFailureRollsBackPayment(t *testing.T) {
	svc, pool := setup(t)
	ctx := context.Background()

	setupOutboxFailureTrigger(t, pool)
	key := "it-rollback-" + uuid.New().String()

	_, err := svc.CreatePayment(ctx, newRequest(t, key, 7500))
	if err == nil {
		t.Fatal("CreatePayment succeeded despite the outbox insert failing; " +
			"a payment was committed that can never be published")
	}
	if code := status.Code(err); code != codes.Unavailable && code != codes.Internal {
		t.Errorf("gRPC code = %s, want Unavailable or Internal", code)
	}

	if got := countRows(t, pool, "payments", key); got != 0 {
		t.Errorf("payments rows = %d, want 0; the transaction did not roll back", got)
	}
	if got := countRows(t, pool, "outbox", key); got != 0 {
		t.Errorf("outbox rows = %d, want 0", got)
	}
}

// A client retry must not produce a second event.
//
// This is the defect that most outbox implementations ship: writing the outbox row
// unconditionally, so every retry of an already-recorded payment emits another
// payment.created and consumers reconcile a movement that never happened.
func TestDuplicateRequestEmitsExactlyOneEvent(t *testing.T) {
	svc, pool := setup(t)
	ctx := context.Background()
	key := "it-dup-" + uuid.New().String()

	first, err := svc.CreatePayment(ctx, newRequest(t, key, 5000))
	if err != nil {
		t.Fatalf("first CreatePayment: %v", err)
	}
	second, err := svc.CreatePayment(ctx, newRequest(t, key, 5000))
	if err != nil {
		t.Fatalf("duplicate CreatePayment: %v", err)
	}

	if first.GetPaymentId() != second.GetPaymentId() {
		t.Errorf("payment_id changed across retries: %s vs %s", first.GetPaymentId(), second.GetPaymentId())
	}
	if got := countRows(t, pool, "payments", key); got != 1 {
		t.Errorf("payments rows = %d, want 1", got)
	}
	if got := countRows(t, pool, "outbox", key); got != 1 {
		t.Errorf("outbox rows = %d, want 1; a duplicate request emitted an extra event", got)
	}
}

// Reusing a key with different parameters is rejected and must emit nothing.
func TestConflictingRequestEmitsNoEvent(t *testing.T) {
	svc, pool := setup(t)
	ctx := context.Background()
	key := "it-conflict-" + uuid.New().String()

	if _, err := svc.CreatePayment(ctx, newRequest(t, key, 5000)); err != nil {
		t.Fatalf("first CreatePayment: %v", err)
	}

	_, err := svc.CreatePayment(ctx, newRequest(t, key, 9999))
	if err == nil {
		t.Fatal("conflicting request was accepted")
	}
	if code := status.Code(err); code != codes.AlreadyExists {
		t.Errorf("gRPC code = %s, want AlreadyExists", code)
	}

	if got := countRows(t, pool, "outbox", key); got != 1 {
		t.Errorf("outbox rows = %d, want 1; a rejected request emitted an event", got)
	}
}

// The staged payload must be the envelope defined by the events.v1 contract, and its
// occurred_at must equal the payment row's created_at so the two cannot disagree.
func TestStagedPayloadMatchesContract(t *testing.T) {
	svc, pool := setup(t)
	ctx := context.Background()
	key := "it-payload-" + uuid.New().String()

	resp, err := svc.CreatePayment(ctx, newRequest(t, key, 4200))
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}

	var payload []byte
	var eventType string
	var eventVersion int
	var aggregateID string
	var outboxStatus string
	err = pool.QueryRow(ctx, `
		SELECT payload, event_type, event_version, aggregate_id, status
		FROM outbox WHERE aggregate_id = $1`, resp.GetPaymentId(),
	).Scan(&payload, &eventType, &eventVersion, &aggregateID, &outboxStatus)
	if err != nil {
		t.Fatalf("read staged event: %v", err)
	}

	if eventType != outbox.EventTypePaymentCreated {
		t.Errorf("event_type = %q, want %q", eventType, outbox.EventTypePaymentCreated)
	}
	if eventVersion != int(outbox.EventVersionPaymentCreated) {
		t.Errorf("event_version = %d, want %d", eventVersion, outbox.EventVersionPaymentCreated)
	}
	if aggregateID != resp.GetPaymentId() {
		t.Errorf("aggregate_id = %q, want %q", aggregateID, resp.GetPaymentId())
	}
	if outboxStatus != "PENDING" {
		t.Errorf("status = %q, want PENDING", outboxStatus)
	}

	event, err := outbox.Unmarshal(payload)
	if err != nil {
		t.Fatalf("staged payload is not a valid events.v1 envelope: %v\n%s", err, payload)
	}
	if event.GetPaymentCreated().GetPaymentId() != resp.GetPaymentId() {
		t.Errorf("payload payment_id = %q, want %q", event.GetPaymentCreated().GetPaymentId(), resp.GetPaymentId())
	}
	if event.GetCorrelationId() != key {
		t.Errorf("correlation_id = %q, want the idempotency key %q", event.GetCorrelationId(), key)
	}
	if got := event.GetPaymentCreated().GetAmountCents(); got != 4200 {
		t.Errorf("amount_cents = %d, want 4200", got)
	}
	if event.GetEventId() == "" {
		t.Error("event_id is empty; consumers could not deduplicate")
	}

	var paymentCreatedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT created_at FROM payments WHERE id=$1`, resp.GetPaymentId()).Scan(&paymentCreatedAt); err != nil {
		t.Fatalf("read payment created_at: %v", err)
	}
	if !event.GetOccurredAt().AsTime().Equal(paymentCreatedAt) {
		t.Errorf("occurred_at %v != payment created_at %v; the app clock must not be used",
			event.GetOccurredAt().AsTime(), paymentCreatedAt)
	}
}

// Concurrent requests carrying the same idempotency key must produce one payment and
// one event. The UNIQUE index serializes them; the loser takes the duplicate path.
func TestConcurrentDuplicateRequestsEmitOneEvent(t *testing.T) {
	svc, pool := setup(t)
	key := "it-concurrent-" + uuid.New().String()

	const workers = 8
	type result struct {
		paymentID string
		err       error
	}
	results := make(chan result, workers)

	start := time.Now()
	for i := 0; i < workers; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			resp, err := svc.CreatePayment(ctx, newRequest(t, key, 5000))
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{paymentID: resp.GetPaymentId()}
		}()
	}

	var firstID string
	for i := 0; i < workers; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("concurrent CreatePayment failed: %v", res.err)
		}
		if firstID == "" {
			firstID = res.paymentID
			continue
		}
		if res.paymentID != firstID {
			t.Errorf("concurrent requests produced different payment_ids: %s vs %s", res.paymentID, firstID)
		}
	}

	if got := countRows(t, pool, "payments", key); got != 1 {
		t.Errorf("payments rows = %d, want 1", got)
	}
	if got := countRows(t, pool, "outbox", key); got != 1 {
		t.Errorf("outbox rows = %d, want 1; concurrency emitted duplicate events", got)
	}
	t.Logf("%d concurrent requests completed in %v", workers, time.Since(start))
}

// setupOutboxFailureTrigger installs a trigger that makes every outbox insert raise,
// forcing the transaction to roll back. Using a trigger keeps the production code free
// of test-only branches.
func setupOutboxFailureTrigger(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_fail_outbox_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'simulated outbox failure';
		END;
		$$;
		DROP TRIGGER IF EXISTS test_fail_outbox ON outbox;
		CREATE TRIGGER test_fail_outbox
			BEFORE INSERT ON outbox
			FOR EACH ROW EXECUTE FUNCTION test_fail_outbox_insert();
	`)
	if err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, dropErr := pool.Exec(cleanupCtx, `DROP TRIGGER IF EXISTS test_fail_outbox ON outbox`); dropErr != nil {
			t.Logf("cleanup: drop trigger failed: %v", dropErr)
		}
	})
}