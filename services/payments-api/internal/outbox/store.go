package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Status is the lifecycle state of an outbox row.
type Status string

const (
	// StatusPending means the event has not yet been handed to Kafka.
	StatusPending Status = "PENDING"
	// StatusPublished means the broker acknowledged the event.
	StatusPublished Status = "PUBLISHED"
	// StatusFailed means the event was routed to the dead-letter topic and will not be
	// retried.
	StatusFailed Status = "FAILED"
)

// Record is a claimed outbox row awaiting publication.
type Record struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	EventVersion  int
	// Payload is the raw protojson envelope. The relayer forwards it verbatim rather than
	// decoding it: it was written by the payment transaction and the contract version is
	// already recorded in EventVersion, so re-encoding here would risk a silent difference
	// between what was stored and what consumers receive.
	Payload []byte

	// Attempts is the number of publish attempts made so far, including this one.
	Attempts int

	// LeaseUntil is when this row becomes claimable again. Claiming pushes it forward so
	// no other relayer picks the row up while this one publishes it.
	LeaseUntil time.Time

	CreatedAt time.Time
}

// Store provides the database operations the relayer needs.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store over a connection pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ErrNoWork signals that no eligible rows were found, which is a normal idle condition
// rather than a failure.
var ErrNoWork = errors.New("no outbox rows available")

// Claim atomically leases up to limit eligible rows for publication.
//
// WHY THE LEASE EXISTS:
// Publishing happens OUTSIDE this transaction. Without the lease, a second relayer would
// see the same still-PENDING row on its next poll and publish it concurrently, duplicating
// the event. Pushing next_attempt_at forward in the same statement that claims the row
// makes the claim exclusive for the lease duration, and doubles as the retry backoff.
//
// WHY SKIP LOCKED:
// It lets several relayer replicas poll the same table without blocking each other. A
// plain FOR UPDATE would serialise them behind the first row, so N replicas would deliver
// no more throughput than one.
//
// The returned lease is intentionally short. If a relayer dies mid-publish, the row
// becomes claimable again after the lease expires rather than being stranded.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error) {
	const query = `
		WITH claimed AS (
			SELECT id
			FROM outbox
			WHERE status = 'PENDING'
			  AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		SET attempts = o.attempts + 1,
		    next_attempt_at = now() + $2::interval
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.event_version,
		          o.payload, o.attempts, o.next_attempt_at, o.created_at
	`

	rows, err := s.pool.Query(ctx, query, limit, formatInterval(lease))
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(
			&r.ID,
			&r.AggregateType,
			&r.AggregateID,
			&r.EventType,
			&r.EventVersion,
			&r.Payload,
			&r.Attempts,
			&r.LeaseUntil,
			&r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan claimed outbox row: %w", err)
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox rows: %w", err)
	}

	if len(records) == 0 {
		return nil, ErrNoWork
	}
	return records, nil
}

// MarkPublished records a successful delivery. published_at is the database clock so it
// cannot disagree with the event's own occurred_at across replicas.
func (s *Store) MarkPublished(ctx context.Context, id string) error {
	const query = `UPDATE outbox SET status = 'PUBLISHED', published_at = now(), last_error = NULL WHERE id = $1`
	if _, err := s.pool.Exec(ctx, query, id); err != nil {
		return fmt.Errorf("mark outbox row published: %w", err)
	}
	return nil
}

// MarkRetry records a transient failure and schedules the next attempt.
//
// The row stays PENDING so it is retried, and last_error is retained for operators. The
// backoff is written into next_attempt_at rather than slept in the relayer, so a slow event
// never occupies a worker.
func (s *Store) MarkRetry(ctx context.Context, id, reason string, backoff time.Duration) error {
	const query = `
		UPDATE outbox
		SET last_error = $2,
		    next_attempt_at = now() + $3::interval
		WHERE id = $1
	`
	if _, err := s.pool.Exec(ctx, query, id, reason, formatInterval(backoff)); err != nil {
		return fmt.Errorf("schedule outbox retry: %w", err)
	}
	return nil
}

// MarkFailed records a terminal failure. The event has already been routed to the
// dead-letter topic by the caller, so the row leaves the retry path permanently.
//
// The caller MUST NOT mark a row failed until the DLQ write succeeded. Marking it failed
// after a failed DLQ write would drop the event with no record of it anywhere.
func (s *Store) MarkFailed(ctx context.Context, id, reason string) error {
	const query = `UPDATE outbox SET status = 'FAILED', last_error = $2 WHERE id = $1`
	if _, err := s.pool.Exec(ctx, query, id, reason); err != nil {
		return fmt.Errorf("mark outbox row failed: %w", err)
	}
	return nil
}

// PendingDepth returns the number of events still awaiting publication. Exposed so the
// relayer can export a backlog gauge without a second query per batch.
func (s *Store) PendingDepth(ctx context.Context) (int, error) {
	const query = `SELECT count(*) FROM outbox WHERE status = 'PENDING'`
	var n int
	if err := s.pool.QueryRow(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending outbox rows: %w", err)
	}
	return n, nil
}

// OldestPendingAge returns how long the oldest unpublished event has been waiting. This is
// the metric that matters operationally: depth alone does not tell you whether the relayer
// is keeping up.
func (s *Store) OldestPendingAge(ctx context.Context) (time.Duration, error) {
	const query = `SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0) FROM outbox WHERE status = 'PENDING'`
	var seconds float64
	if err := s.pool.QueryRow(ctx, query).Scan(&seconds); err != nil {
		return 0, fmt.Errorf("measure oldest pending outbox row: %w", err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

// formatInterval renders a duration as a PostgreSQL interval literal.
//
// The value is always passed as a parameter rather than interpolated into SQL, so however
// it was derived there is no injection surface.
func formatInterval(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return fmt.Sprintf("%f seconds", d.Seconds())
}
