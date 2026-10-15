package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// PaymentsTotal tracks total payment creation attempts partitioned by result outcome.
	// Results: created, duplicate, conflict, invalid, error.
	PaymentsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "resilientbank_payments_total",
			Help: "Total count of payment attempts by result outcome (created, duplicate, conflict, invalid, error).",
		},
		[]string{"result"},
	)

	// PaymentDuration tracks end-to-end latency of CreatePayment requests.
	PaymentDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "resilientbank_payment_duration_seconds",
			Help:    "Latency of CreatePayment requests in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)

	// DependencyErrorsTotal tracks failures encountered when calling external dependencies.
	// Dependencies: postgres, redis.
	DependencyErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "resilientbank_dependency_errors_total",
			Help: "Total count of external dependency communication errors (postgres, redis).",
		},
		[]string{"dependency"},
	)
)

// Outbox metrics. These belong to the relayer process but live in this package so both
// binaries share one set of metric names and one registration point.
var (
	// OutboxPending is the number of events still awaiting publication. It is the primary
	// signal that the relayer is falling behind; a steadily growing value means events are
	// being committed faster than they can be delivered.
	OutboxPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "resilientbank_outbox_pending",
			Help: "Number of events in the outbox still awaiting publication to Kafka.",
		},
	)

	// OutboxOldestAgeSeconds is how long the oldest unpublished event has been waiting.
	// Depth alone is misleading: a deep backlog that is draining fast is healthy, while a
	// shallow one whose oldest item is hours old means the relayer has stalled.
	OutboxOldestAgeSeconds = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "resilientbank_outbox_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest unpublished event in the outbox.",
		},
	)

	// OutboxPublishedTotal counts publish attempts by outcome.
	// Results: published, retry, terminal, exhausted, mark_failed.
	//
	// A persistent non-zero mark_failed rate means events are reaching Kafka but their
	// rows are not being updated, which causes duplicate redelivery.
	OutboxPublishedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "resilientbank_outbox_published_total",
			Help: "Total outbox publish attempts by outcome (published, retry, terminal, exhausted, mark_failed).",
		},
		[]string{"result"},
	)

	// OutboxDeadLetterTotal counts events routed to the dead-letter topic.
	// Outcomes: routed, publish_failed.
	//
	// publish_failed means the event could NOT be dead-lettered and the row was left
	// PENDING. That is an operational problem: the event is neither delivered nor
	// quarantined.
	OutboxDeadLetterTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "resilientbank_outbox_dead_letter_total",
			Help: "Total events routed to the dead-letter topic by outcome (routed, publish_failed).",
		},
		[]string{"outcome"},
	)

	// OutboxPublishDuration tracks how long a single publish attempt takes.
	OutboxPublishDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "resilientbank_outbox_publish_duration_seconds",
			Help:    "Latency of a single outbox publish attempt in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)

	// OutboxBatchSize tracks how many events each claim returned. A shrinking batch with a
	// growing backlog indicates claim contention between relayer replicas.
	OutboxBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "resilientbank_outbox_claimed_batch_size",
			Help:    "Number of events claimed per outbox poll.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
	)
)
