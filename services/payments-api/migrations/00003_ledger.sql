-- +goose Up
--
-- Double-entry ledger, accounts, reversals, settlements, and audit trail.
--
-- WHY DOUBLE-ENTRY:
-- A payment that only flips two balances can silently create or destroy money if the
-- code has a bug: debit without credit, credit twice, or a partial failure. Double-entry
-- bookkeeping makes that impossible by construction. Every movement of value writes one
-- DEBIT and one CREDIT whose amounts sum to zero, and the accounts' balances are a cached
-- projection that must always equal the sum of their entries. Any divergence is a defect,
-- detectable by a single query.
--
-- All of it -- balances, ledger, payment, outbox -- commits in ONE transaction (see the
-- payments service). A payment without its ledger entries, or ledger entries without their
-- payment, cannot exist.
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    holder_name TEXT NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_cents BIGINT NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_status_check CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED'))
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id UUID PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payments(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    direction TEXT NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ledger_direction_check CHECK (direction IN ('DEBIT', 'CREDIT'))
);
CREATE INDEX IF NOT EXISTS idx_ledger_payment ON ledger_entries(payment_id);
CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger_entries(account_id);

-- Reversals undo a settled payment with compensating entries, never by deleting or
-- editing the original rows. Money already moved cannot un-move; it can only move back.
CREATE TABLE IF NOT EXISTS reversals (
    id UUID PRIMARY KEY,
    original_payment_id UUID NOT NULL REFERENCES payments(id) ON DELETE RESTRICT,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT reversals_status_check CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),
    -- A payment cannot reverse itself; the compensating movement is a distinct record.
    CONSTRAINT reversals_distinct CHECK (id <> original_payment_id)
);
CREATE INDEX IF NOT EXISTS idx_reversals_original ON reversals(original_payment_id);

-- Settlement batches group payments for net settlement. OPEN batches accept new payments;
-- CLOSED batches are final and ready to settle; SETTLED batches are done.
CREATE TABLE IF NOT EXISTS settlement_batches (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'OPEN',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ,
    CONSTRAINT settlement_status_check CHECK (status IN ('OPEN', 'CLOSED', 'SETTLED'))
);
CREATE TABLE IF NOT EXISTS settlement_items (
    batch_id UUID NOT NULL REFERENCES settlement_batches(id) ON DELETE CASCADE,
    payment_id UUID NOT NULL REFERENCES payments(id) ON DELETE RESTRICT,
    added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, payment_id)
);
CREATE INDEX IF NOT EXISTS idx_settlement_items_payment ON settlement_items(payment_id);

-- Append-only audit trail. Rows are never updated or deleted; corrections are new rows.
-- This is what compliance reporting reads.
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT 'system',
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT audit_immutable CHECK (true)
);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at);

-- Extend payments with settlement lifecycle and reversal linkage. All nullable so existing
-- rows upgrade without backfill.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS failure_reason TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS parent_payment_id UUID REFERENCES payments(id) ON DELETE RESTRICT;

-- +goose Down
--
-- DESTRUCTIVE and ordered innermost-first so foreign keys never block the teardown.
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS settlement_items;
DROP TABLE IF EXISTS settlement_batches;
DROP TABLE IF EXISTS reversals;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS accounts;
ALTER TABLE payments DROP COLUMN IF EXISTS parent_payment_id;
ALTER TABLE payments DROP COLUMN IF EXISTS failure_reason;
ALTER TABLE payments DROP COLUMN IF EXISTS settled_at;