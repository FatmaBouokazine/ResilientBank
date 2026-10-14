-- Transactional outbox: durable staging for domain events awaiting publication to Kafka.
--
-- A payment row and its outbox row are inserted inside one transaction, so a committed
-- payment can never lack its event and an event can never describe an absent payment.
-- See ADR 0002.
--
-- Status lifecycle:
--   PENDING   - awaiting publication; eligible for the relayer when next_attempt_at <= now()
--   PUBLISHED - successfully handed to Kafka
--   FAILED    - terminal failure, routed to the dead-letter topic; not retried
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY,

    -- Domain entity the event describes. Denormalised onto the row so the relayer can
    -- route and partition without joining back to payments.
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,

    -- Dotted event name (e.g. "payment.created") and the payload schema version.
    -- Consumers branch on event_type and must tolerate unknown event_version values.
    event_type TEXT NOT NULL,
    event_version INT NOT NULL,

    -- protojson-encoded events.v1.PaymentEvent envelope. Stored as JSONB so operators
    -- can inspect event contents directly in psql without decoding a binary format.
    payload JSONB NOT NULL,

    status TEXT NOT NULL DEFAULT 'PENDING',

    -- Retry bookkeeping. attempts counts publish attempts; next_attempt_at implements
    -- exponential backoff by pushing eligibility into the future instead of sleeping.
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_status_check CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    -- An event that never carries its payload is unrouteable and must never exist.
    CONSTRAINT outbox_event_type_check CHECK (event_type <> ''),
    CONSTRAINT outbox_event_version_check CHECK (event_version > 0)
);

-- Partial index backing the relayer's polling query. Only rows still awaiting
-- publication are indexed, so the index stays proportional to backlog rather than
-- to total table size, which grows forever as published rows accumulate.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (next_attempt_at)
    WHERE status = 'PENDING';

-- Supports per-aggregate history lookups and, later, partition-key auditing.
CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox (aggregate_type, aggregate_id);