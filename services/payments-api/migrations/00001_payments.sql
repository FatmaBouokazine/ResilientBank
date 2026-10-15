-- +goose Up
--
-- Baseline payments table from Phase 1.
--
-- CREATE TABLE IF NOT EXISTS is deliberate and load-bearing. Developers who ran the
-- stack before the goose runner existed already have this table populated but no
-- schema_migrations row recording it. Keeping the statement idempotent means applying
-- this migration to such a database is a no-op that preserves their data, rather than
-- an error that forces them to delete it.
CREATE TABLE IF NOT EXISTS payments (
    id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payments_idempotency_key ON payments(idempotency_key);

-- +goose Down
--
-- DESTRUCTIVE. Removes the payments table and every payment it contains.
--
-- There is deliberately no confirmation prompt here: goose applies files literally, so
-- the guardrail belongs in the operator's workflow. Never run this against a database
-- holding real transactions.
DROP TABLE IF EXISTS payments;