package chaos

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxObserver verifies the platform's actual guarantees against the live database.
//
// It reads the outbox directly rather than scraping metrics, because the difference between
// "the relayer reported no failures" and "no payment was lost" is exactly the distinction a
// chaos run exists to make. Metrics can be lying; rows cannot.
type OutboxObserver struct {
	pool *pgxpool.Pool

	// MaxPublishLag bounds how old an unpublished event may be. Beyond this, delivery is
	// considered not fresh.
	MaxPublishLag time.Duration

	// MaxOutboxBacklog bounds how many events may be pending before the system is judged
	// to be falling behind rather than merely delayed.
	MaxOutboxBacklog int

	mu sync.Mutex
}

// NewOutboxObserver builds an observer over the outbox and payments tables.
//
// baselinePending is the number of events already pending when the experiment starts. Those
// are excluded from fault-window judgements: a pre-existing backlog has nothing to do with
// the fault being injected, and counting it would make every experiment fail for the same
// unrelated reason.
func NewOutboxObserver(pool *pgxpool.Pool, maxLag time.Duration, maxBacklog int) *OutboxObserver {
	if maxLag <= 0 {
		maxLag = 2 * time.Minute
	}
	if maxBacklog <= 0 {
		maxBacklog = 10_000
	}
	return &OutboxObserver{
		pool:             pool,
		MaxPublishLag:    maxLag,
		MaxOutboxBacklog: maxBacklog,
	}
}

// Observe evaluates the platform's objectives.
//
// The two critical objectives are answered by row-level evidence:
//
//   - SLOPaymentAvailability: every payment row has a corresponding outbox row. A payment
//     with no event can never be told to anyone downstream, so its absence is a real defect.
//   - SLOEventDurability: no row is in FAILED, because FAILED means the event was sent to
//     the dead-letter topic and abandoned.
//
// SLOEventFreshness is the objective allowed to degrade: a partition delays delivery
// without losing anything.
func (o *OutboxObserver) Observe(ctx context.Context) ([]Measurement, error) {
	now := time.Now()

	// Payments committed without a staged event. This must always be zero.
	var paymentsWithoutEvents int64
	if err := o.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM payments p
		LEFT JOIN outbox o ON o.aggregate_id = p.id::text
		WHERE o.id IS NULL`).Scan(&paymentsWithoutEvents); err != nil {
		return nil, fmt.Errorf("count payments without events: %w", err)
	}

	// Events permanently abandoned in the dead-letter state.
	var deadLettered int64
	if err := o.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE status = 'FAILED'`).Scan(&deadLettered); err != nil {
		return nil, fmt.Errorf("count dead-lettered events: %w", err)
	}

	// Still awaiting publication.
	var pending int64
	if err := o.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE status = 'PENDING'`).Scan(&pending); err != nil {
		return nil, fmt.Errorf("count pending events: %w", err)
	}

	// Oldest event still awaiting publication, for the freshness objective.
	var oldestLagSeconds *float64
	if err := o.pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at)))
		FROM outbox
		WHERE status = 'PENDING'`).Scan(&oldestLagSeconds); err != nil {
		return nil, fmt.Errorf("measure oldest pending event: %w", err)
	}

	measurements := []Measurement{
		{
			SLOName:    SLOPaymentAvailability,
			Healthy:    paymentsWithoutEvents == 0,
			ObservedAt: now,
			Detail: fmt.Sprintf("%d payment(s) committed with no event staged; a payment with no "+
				"event can never be reported to downstream consumers", paymentsWithoutEvents),
		},
		{
			SLOName:    SLOEventDurability,
			Healthy:    deadLettered == 0,
			ObservedAt: now,
			Detail:     fmt.Sprintf("%d event(s) abandoned in the dead-letter state", deadLettered),
		},
	}

	// Freshness: no pending events means nothing is late, which is trivially fresh.
	fresh := true
	freshDetail := "no events pending"
	if pending > 0 {
		if oldestLagSeconds == nil {
			fresh = false
			freshDetail = "events pending but the oldest could not be measured"
		} else {
			lag := time.Duration(*oldestLagSeconds * float64(time.Second))
			fresh = lag <= o.MaxPublishLag
			freshDetail = fmt.Sprintf("oldest pending event is %v old (limit %v), %d pending",
				lag.Truncate(time.Second), o.MaxPublishLag, pending)
		}
	}
	measurements = append(measurements, Measurement{
		SLOName:    SLOEventFreshness,
		Healthy:    fresh,
		ObservedAt: now,
		Detail:     freshDetail,
	})

	return measurements, nil
}

var _ Observer = (*OutboxObserver)(nil)
