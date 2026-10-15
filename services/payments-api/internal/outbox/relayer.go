package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	rbkafka "github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/kafka"
	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/metrics"
)

// Relayer drains the outbox to Kafka.
//
// It runs as a separate process from the payment API so that broker latency, producer
// stalls, and backpressure can never reach the gRPC ingestion path. See ADR 0002.
type Relayer struct {
	store     *Store
	publisher rbkafka.Publisher
	cfg       RelayerConfig
	logger    *slog.Logger

	// now is injectable so tests can control time without sleeping.
	now func() time.Time
}

// RelayerConfig holds the relayer's runtime behaviour.
type RelayerConfig struct {
	// Topic receives payment events.
	Topic string
	// TopicDLQ receives events that must not be retried.
	TopicDLQ string

	BatchSize int

	// PollInterval is how often the relayer looks for work when the previous poll found
	// nothing.
	PollInterval time.Duration

	// Lease is how long a claimed row stays invisible to other relayers. It must exceed a
	// normal publish round-trip or a healthy row could be claimed twice.
	Lease time.Duration

	// MaxAttempts is how many publish attempts an event gets before it is dead-lettered.
	MaxAttempts int

	Backoff BackoffPolicy
}

// DefaultRelayerConfig returns production defaults.
func DefaultRelayerConfig() RelayerConfig {
	return RelayerConfig{
		Topic:        "payments.events.v1",
		TopicDLQ:     "payments.events.v1.dlq",
		BatchSize:    100,
		PollInterval: 500 * time.Millisecond,
		Lease:        30 * time.Second,
		MaxAttempts:  10,
		Backoff:      DefaultBackoffPolicy(),
	}
}

// NewRelayer constructs a Relayer.
func NewRelayer(store *Store, publisher rbkafka.Publisher, cfg RelayerConfig, logger *slog.Logger) *Relayer {
	return &Relayer{
		store:     store,
		publisher: publisher,
		cfg:       cfg,
		logger:    logger,
		now:       time.Now,
	}
}

// Run polls the outbox until ctx is cancelled.
//
// It returns nil on a clean shutdown. In-flight batches are finished before returning, so
// a graceful stop never abandons a claimed row mid-publish.
func (r *Relayer) Run(ctx context.Context) error {
	r.logger.Info("outbox relayer started",
		"topic", r.cfg.Topic,
		"dlq_topic", r.cfg.TopicDLQ,
		"batch_size", r.cfg.BatchSize,
		"poll_interval", r.cfg.PollInterval,
		"max_attempts", r.cfg.MaxAttempts,
		"publisher_enabled", r.publisher.Enabled(),
	)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		// Drain immediately, then on each tick, so a backlog is cleared at full rate
		// rather than one batch per interval.
		claimed, err := r.drainOnce(ctx)
		if claimed > 0 {
			metrics.OutboxBatchSize.Observe(float64(claimed))
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			r.logger.ErrorContext(ctx, "outbox drain cycle failed", "error", err)
		}

		if ctx.Err() != nil {
			break
		}

		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}

	// Release anything the producer still holds before the process exits.
	if remaining := r.publisher.Flush(5 * time.Second); remaining > 0 {
		r.logger.Warn("producer still had buffered records at shutdown", "remaining", remaining)
	}

	r.logger.Info("outbox relayer stopped")
	return nil
}

// drainOnce claims and processes a single batch, returning how many records it handled.
func (r *Relayer) drainOnce(ctx context.Context) (int, error) {
	records, err := r.store.Claim(ctx, r.cfg.BatchSize, r.cfg.Lease)
	if err != nil {
		if errors.Is(err, ErrNoWork) {
			r.refreshGauges(ctx)
			return 0, nil
		}
		return 0, err
	}

	for _, record := range records {
		if ctx.Err() != nil {
			// Shutting down. The row keeps its lease and becomes claimable again when it
			// expires, so nothing is lost.
			break
		}
		r.handle(ctx, record)
	}

	r.refreshGauges(ctx)
	return len(records), nil
}

// handle publishes a single record and records the outcome.
func (r *Relayer) handle(ctx context.Context, record Record) {
	start := time.Now()
	headers := r.buildHeaders(record)

	_, publishErr := r.publisher.Publish(ctx, r.cfg.Topic, r.AggregateKey(record), record.Payload, headers)
	metrics.OutboxPublishDuration.Observe(time.Since(start).Seconds())

	if publishErr == nil {
		if err := r.store.MarkPublished(ctx, record.ID); err != nil {
			// The event IS on the broker but the row is not marked. It stays PENDING and is
			// republished after the lease expires, producing a duplicate. That is the
			// documented at-least-once contract: consumers deduplicate on the envelope's
			// event_id. Failing the other way would lose the event instead.
			metrics.OutboxPublishedTotal.WithLabelValues("mark_failed").Inc()
			r.logger.ErrorContext(ctx, "published but failed to mark row; event will be redelivered",
				"outbox_id", record.ID,
				"event_type", record.EventType,
				"error", err,
			)
			return
		}
		metrics.OutboxPublishedTotal.WithLabelValues("published").Inc()
		return
	}

	if !rbkafka.IsRetriable(publishErr) {
		metrics.OutboxPublishedTotal.WithLabelValues("terminal").Inc()
		r.deadLetter(ctx, record, publishErr, "terminal publish failure")
		return
	}

	if record.Attempts >= r.cfg.MaxAttempts {
		metrics.OutboxPublishedTotal.WithLabelValues("exhausted").Inc()
		r.deadLetter(ctx, record, publishErr, "publish attempts exhausted")
		return
	}

	delay := r.cfg.Backoff.Delay(record.Attempts)
	if err := r.store.MarkRetry(ctx, record.ID, publishErr.Error(), delay); err != nil {
		metrics.OutboxPublishedTotal.WithLabelValues("mark_failed").Inc()
		r.logger.ErrorContext(ctx, "failed to schedule retry",
			"outbox_id", record.ID,
			"error", err,
		)
		return
	}

	metrics.OutboxPublishedTotal.WithLabelValues("retry").Inc()
	r.logger.WarnContext(ctx, "publish failed; scheduled retry",
		"outbox_id", record.ID,
		"event_type", record.EventType,
		"attempts", record.Attempts,
		"retry_in", delay,
		"error", publishErr,
	)
}

// deadLetter routes an unpublishable event to the dead-letter topic and only then marks
// the row FAILED.
//
// The ordering is the whole point: if the DLQ write fails the row is left PENDING and
// retried, because marking it FAILED after a failed DLQ write would drop the event with no
// copy anywhere.
func (r *Relayer) deadLetter(ctx context.Context, record Record, cause error, reason string) {
	headers := r.buildHeaders(record)
	headers["x-dead-letter-reason"] = reason
	headers["x-dead-letter-error"] = truncate(cause.Error(), 512)
	headers["x-dead-letter-attempts"] = fmt.Sprintf("%d", record.Attempts)

	dlqKey := r.AggregateKey(record)
	if _, err := r.publisher.Publish(ctx, r.cfg.TopicDLQ, dlqKey, record.Payload, headers); err != nil {
		metrics.OutboxDeadLetterTotal.WithLabelValues("publish_failed").Inc()
		r.logger.ErrorContext(ctx, "failed to write to the dead-letter topic; leaving row PENDING for retry",
			"outbox_id", record.ID,
			"dlq_topic", r.cfg.TopicDLQ,
			"reason", reason,
			"error", err,
		)
		// Leave the row PENDING. Schedule a retry so it is re-examined rather than being
		// stranded with an exhausted attempt count.
		if markErr := r.store.MarkRetry(ctx, record.ID, "dead-letter publish failed: "+err.Error(), r.cfg.Backoff.Delay(record.Attempts)); markErr != nil {
			r.logger.ErrorContext(ctx, "failed to schedule retry after dead-letter failure",
				"outbox_id", record.ID,
				"error", markErr,
			)
		}
		return
	}

	if err := r.store.MarkFailed(ctx, record.ID, reason+": "+cause.Error()); err != nil {
		metrics.OutboxDeadLetterTotal.WithLabelValues("mark_failed").Inc()
		r.logger.ErrorContext(ctx, "dead-lettered but failed to mark row FAILED",
			"outbox_id", record.ID,
			"error", err,
		)
		return
	}

	metrics.OutboxDeadLetterTotal.WithLabelValues("routed").Inc()
	r.logger.ErrorContext(ctx, "event routed to the dead-letter topic",
		"outbox_id", record.ID,
		"event_type", record.EventType,
		"aggregate_id", record.AggregateID,
		"attempts", record.Attempts,
		"reason", reason,
		"error", cause,
	)
}

// buildHeaders produces the metadata carried alongside the event.
//
// The stored payload is forwarded byte for byte; these headers are read by operators and
// by the dead-letter tooling, never by consumers deciding what the event means. That stays
// in the envelope, where buf breaking governs it.
func (r *Relayer) buildHeaders(record Record) map[string]string {
	return map[string]string{
		"x-outbox-id":      record.ID,
		"x-event-type":     record.EventType,
		"x-event-version":  fmt.Sprintf("%d", record.EventVersion),
		"x-aggregate-type": record.AggregateType,
		"x-aggregate-id":   record.AggregateID,
		"x-attempts":       fmt.Sprintf("%d", record.Attempts),
		"x-published-at":   r.now().UTC().Format(time.RFC3339Nano),
	}
}

// AggregateKey returns the partition key for a record.
//
// Keying by aggregate id is what keeps all events for one payment on one partition, so a
// consumer sees them in the order the outbox recorded them.
func (r *Relayer) AggregateKey(record Record) string {
	return record.AggregateID
}

// refreshGauges updates backlog metrics. A failure here must never stop the relayer, since
// the backlog is still being drained underneath it.
func (r *Relayer) refreshGauges(ctx context.Context) {
	depth, err := r.store.PendingDepth(ctx)
	if err != nil {
		metrics.DependencyErrorsTotal.WithLabelValues("postgres").Inc()
		return
	}
	metrics.OutboxPending.Set(float64(depth))

	age, err := r.store.OldestPendingAge(ctx)
	if err != nil {
		return
	}
	metrics.OutboxOldestAgeSeconds.Set(age.Seconds())
}

// truncate bounds a header value. Kafka has a maximum header size, and an unbounded error
// string would cause the dead-letter write itself to fail.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
