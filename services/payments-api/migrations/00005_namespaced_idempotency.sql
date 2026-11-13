-- +goose Up
--
-- Namespace existing idempotency keys by the region that owns them. Every payment written
-- before this migration belongs to the default region, so prefixing with 'A:' aligns old
-- rows with what the API now writes and prevents a region B key from colliding with an
-- existing region A key.
UPDATE payments
SET idempotency_key = 'A:' || idempotency_key
WHERE idempotency_key NOT LIKE 'A:%'
  AND idempotency_key NOT LIKE 'B:%';

-- +goose Down
--
-- UPDATE payments
-- SET idempotency_key = substr(idempotency_key, 3)
-- WHERE idempotency_key LIKE 'A:%';
