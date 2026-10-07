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
