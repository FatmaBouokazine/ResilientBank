-- +goose Up
--
-- Region scoping for accounts and ledger entries. Each region owns its own account
-- balances and ledger stream; replicated transfers are applied inside the receiving
-- region's scope so the two regions never share balance state.
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT 'A';
ALTER TABLE ledger_entries ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT 'A';

-- +goose Down
--
-- DROP COLUMN IF EXISTS ledger_entries.region;
-- DROP COLUMN IF EXISTS accounts.region;
