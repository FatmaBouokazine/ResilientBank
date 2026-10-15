# ADR 0002: Transactional Outbox for Domain Event Publication

## Status

Accepted

## Context

Phase 2 introduces an event-driven core: payments ingested over gRPC must also be published to Kafka so that downstream consumers (ledger reconciliation, ML anomaly detection, analytics) can react without polling the database.

The core problem is that a payment lives in PostgreSQL while the event must land in Kafka. These are two independent systems with no shared transaction coordinator, so any attempt to write to both sequentially produces a gap.

### Failure modes we must prevent

1. **Write database, then publish.** If the process crashes between the payment commit and the Kafka publish, the payment exists but no event is ever emitted. The money moved and no consumer ever learns about it. For a bank this is an unrecoverable reconciliation break.
2. **Publish, then write database.** If the process crashes after publishing but before the payment commits, consumers act on a payment that does not exist. This is strictly worse: downstream systems have already moved money on a phantom transaction.

### Design requirements

1. A committed payment must eventually produce exactly one event, with no manual reconciliation step.
2. Ingestion latency must not depend on Kafka availability. A broker outage must never cause a payment to fail.
3. Consumers must be able to detect redelivery and deduplicate.
4. The event contract must be versioned so producers and consumers can be deployed independently across regions.

## Decision

We adopt the **transactional outbox pattern**. The payment row and its pending event are written inside a single PostgreSQL transaction; publication to Kafka is performed asynchronously by a separate relayer process that polls the outbox table.

### 1. Atomicity lives in PostgreSQL, not Kafka

The payment `INSERT` and the outbox `INSERT` commit or roll back together. This is the only guarantee that matters, and it does not depend on Kafka being reachable.

We deliberately do **not** use Kafka transactions (the `transactional.id` producer API). Doing so would require the Kafka transaction to stay open across the database commit, which reintroduces failure mode 2 above and couples database commit latency to broker round-trips. The database is already the source of truth for payments; introducing a second transaction coordinator would weaken, not strengthen, the guarantee.

### 2. Event emission is conditional on genuine creation

The payment insert uses `ON CONFLICT (idempotency_key) DO NOTHING ... RETURNING id, created_at`. An outbox row is written **only** when the statement returns a row.

This is the single most important detail in the implementation. `DO NOTHING` returning zero rows means the request was a *duplicate* of an already-recorded payment. Emitting an event for a duplicate would cause every client retry to produce an additional `payment.created`, flooding the topic with phantom movements. Consumers would then reconcile a transfer that never happened.

### 3. Isolation level

The transaction runs at `READ COMMITTED`, PostgreSQL's default. Concurrent requests sharing an idempotency key are serialized by the unique index on `idempotency_key`: the second transaction blocks on the index until the first commits, then observes the conflict and takes the duplicate path.

`SERIALIZABLE` is explicitly rejected. It would add serialization failures that must be caught and retried, introducing abort storms under exactly the contention pattern (client retry storms on a single key) the idempotency layer exists to absorb, in exchange for no additional correctness.

### 4. Occurrence timestamps come from the database

`occurred_at` is populated from the payment row's `created_at` as returned by `INSERT ... RETURNING`, never from the application's clock. Application and database clocks drift, and a region running ahead of its database would emit events whose ordering contradicts the records they describe.

### 5. Relay is a separate process

Publication is performed by a dedicated relayer rather than an in-process goroutine. This keeps Kafka latency, broker backpressure, and producer stalls off the gRPC ingestion path entirely, and allows the relayer to be scaled, restarted, or rolled back independently of the payment API.

The relayer claims rows with `SELECT ... FOR UPDATE SKIP LOCKED`, which permits multiple relayer replicas to poll concurrently without contending, and uses `event_id` as the partition key so all events for one payment retain their relative order.

### 6. Contract and versioning

The event contract is defined in `proto/events/v1/payment_events.proto` and serialized with `protojson` into the outbox's `JSONB` payload. Protos give us schema evolution rules enforced by `buf breaking`; `protojson` keeps stored payloads human-inspectable in `psql`.

Two serialization details are load-bearing and are covered by tests:

- `UseProtoNames` is enabled so serialized keys are `snake_case`, matching the surrounding schema.
- `int64` fields such as `amount_cents` are encoded as **JSON strings** (`"5000"`), because the protobuf JSON mapping encodes 64-bit integers as strings to avoid precision loss in JavaScript consumers. Consumers using a generic JSON parser must account for this.

## Consequences

### Positive

- A committed payment can never lack its event, and an emitted event can never describe a payment that does not exist.
- Kafka downtime degrades gracefully: ingestion continues, unpublished events accumulate in the outbox, and the relayer drains them when the broker returns.
- Consumers see at-least-once delivery with a stable `event_id` for deduplication, and can be deployed independently across regions.

### Negative / Trade-offs

- Delivery is no longer synchronous with the API response, so callers cannot assume an event exists the instant a payment is accepted.
- Duplicate events are possible on relayer crash between a successful Kafka publish and the `PUBLISHED` marking, which is why consumers must deduplicate on `event_id`.
- The outbox table grows continuously and requires a retention or archival policy that is not yet defined.
- Serialization happens while the database transaction is open. This is a deliberate fail-closed choice — a payment whose event cannot be serialized must not commit — and costs microseconds of CPU.
- Schema migrations are currently applied only on database volume initialization. A production deployment requires a real migration runner; see Follow-ups.

## Follow-ups

- Adopt a migration runner (`goose` or `golang-migrate`); `docker-entrypoint-initdb.d` only applies scripts to an empty volume.
- Define outbox retention and archival so the table does not grow without bound.
- Add retry classification and dead-letter routing for events that repeatedly fail to publish.