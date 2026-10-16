-- Seed demo accounts with opening balances.
--
-- NOT a migration: seed data does not belong in versioned schema. Run it explicitly with
-- `make seed` after migrating. It is idempotent (INSERT ... ON CONFLICT DO NOTHING) so
-- re-running it never duplicates or alters existing balances.
--
-- Balances are opening demo funds, not real money. A production system funds accounts
-- through deposits, never through seed scripts.
INSERT INTO accounts (id, holder_name, currency, balance_cents, status) VALUES
    ('acc-alice', 'Alice Doe',   'EUR', 100000, 'ACTIVE'),
    ('acc-bob',   'Bob Smith',   'EUR',  50000, 'ACTIVE'),
    ('acc-carol', 'Carol Jones', 'USD', 200000, 'ACTIVE'),
    ('acc-dave',  'Dave Brown',  'USD',  75000, 'ACTIVE'),
    ('acc-bank',  'ResilientBank Settlement', 'EUR', 1000000000, 'ACTIVE')
ON CONFLICT (id) DO NOTHING;