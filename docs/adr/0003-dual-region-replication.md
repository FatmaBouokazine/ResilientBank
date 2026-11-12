# ADR 0003: Dual-Region Replication with Region-Scoped Streams

## Status

Accepted

## Context

The platform must survive the loss of a whole region without losing a committed payment. The
single-region design already guarantees that a payment and its event are written atomically,
but both live in one PostgreSQL instance and one Kafka cluster. Losing that region loses the
payment.

A second region introduces a harder problem than simply copying rows. Each region can accept
payments independently, the same payment can arrive in both regions, and the two regions can
disagree about state while the network between them is broken. Replication must converge
without a distributed transaction coordinator and without double-moving money.

### Requirements

1. A payment committed in the primary region must eventually exist in the replica region.
2. Applying the same replicated event more than once must not move value twice.
3. A region must keep accepting payments while the other region is unreachable.
4. Replication must not sit on the ingestion path: a slow or broken link must never fail a
   payment that the primary region has already committed.
5. Each region's event stream must be independently consumable so consumers can tell which
   region produced an event.

## Decision

We replicate committed events from region A to region B through a region-scoped Kafka topic,
and the replica applies them idempotently using the source payment's correlation identifier.

### 1. Region-scoped topics

`TopicFor` names the primary region's stream exactly as before (`payments.events.v1`) and
qualifies every other region (`payments.events.v1.B`). `TopicDLQFor` derives the dead-letter
topic from the region-scoped name. Keeping region A on the legacy name avoids a migration of
existing consumers; new regions are unambiguous by construction.

### 2. Replication is part of the relayer, not the request path

Replication is an opt-in flag on the existing outbox relayer
(`KAFKA_REPLICATION_ENABLED`, `KAFKA_REPLICATION_TARGET_REGION`). After the relayer has
published an event to its own region's topic and marked the outbox row `PUBLISHED`, it
forwards the same event to the target region's topic.

The source commit stands even when the forward fails: a failed cross-region publish is logged
and the outbox row is not rolled back. This is the correct failure direction. The primary
region's guarantee is that *its* payment and event are durable; making ingestion depend on a
healthy inter-region link would turn a network partition into a payment outage.

### 3. Idempotent apply on the replica

`ApplyReplicatedPayment` is the only entry point for replicated events. It is idempotent on
the source payment identifier: before applying, it looks the payment up by correlation id and
returns successfully if it already exists. The insert itself also uses
`ON CONFLICT (idempotency_key) DO NOTHING`, so a race between two delivery attempts resolves
the same way as a client retry.

The correlation id is the source payment's idempotency key. Reusing it as the replica's
idempotency key means a payment has one identity in both regions, which is what makes the
duplicate check meaningful.

### 4. Region-scoped accounts and ledger entries

Accounts and ledger entries carry a `region` column, and the balance updates are constrained
to it (`WHERE id = $2 AND region = $3`). A replicated payment debits and credits the rows that
belong to the region that originally committed it. This prevents a region from mutating the
other region's balances during a partition and keeps the ledger auditable per region.

### 5. Convergence instead of split-brain resolution

We deliberately do not implement a consensus protocol between the regions. Two regions
accepting the same payment key converge because both sides key on the same idempotency /
correlation value and the request hash: the second apply observes the existing payment and
does nothing. Two regions accepting *different* payments are not a conflict at all, because
each is applied in its own region's accounts.

This is an availability-first choice. It gives up immediate global consistency for the
ability to keep accepting payments during a partition. The accepted price is that
cross-region balance views can lag until the link recovers.

## Consequences

### Positive

- A committed payment is durable in its own region before replication is attempted, so an
  inter-region outage cannot lose it.
- Replicas can redeliver freely; applying the same event twice is a no-op.
- Consumers can distinguish region A and region B streams and deduplicate on the shared
  payment identity.
- No distributed transaction and no inter-region lock, so the primary's ingestion latency is
  unaffected by the replica.

### Negative / Trade-offs

- Delivery is at-least-once, so consumers must deduplicate on the payment/correlation id.
- Replication is asynchronous. A disaster that destroys region A before its events reach
  region B loses the unreplicated tail. This is a bounded, acknowledged window, not a claim
  of zero data loss across regions.
- The current relayer forwards from the default region to a configured target. Bidirectional
  active-active replication would need loop suppression and a conflict policy that this ADR
  does not define.
- Region-scoped balances mean a global balance is a sum over regions, not a single row. Any
  reporting that assumes one balance per account must change.

## Alternatives considered

- **Kafka MirrorMaker.** Rejected for this phase. It replicates bytes between clusters but
  says nothing about idempotent application into the other region's database, which is where
  double-spend actually happens.
- **Synchronous two-phase commit across regions.** Rejected. It couples payment latency and
  availability to the inter-region link and reintroduces the failure mode the outbox exists
  to prevent.
- **A globally shared database.** Rejected. It removes the whole point of regional
  independence and makes the database a single point of failure again.

## Follow-ups

- Define a conflict policy before enabling bidirectional replication; the current forwarder
  is primary-to-replica only.
- Add loop suppression metadata to replicated events so a future bidirectional setup cannot
  echo events back and forth.
- Measure and publish the replication lag, and alert when it exceeds the acceptable
  data-loss window.
