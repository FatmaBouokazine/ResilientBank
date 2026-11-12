-- +goose Up
--
-- Region linkage for the audit trail. `region` is where the audited action was performed;
-- `origin_region` is where the payment originated when the action was a replicated apply.
-- Keeping both columns lets compliance trace one payment across regions without parsing
-- the details JSON, and makes a replicated write distinguishable from a local one.
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS region TEXT NOT NULL DEFAULT 'A';
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS origin_region TEXT;
CREATE INDEX IF NOT EXISTS idx_audit_region ON audit_log(region);
CREATE INDEX IF NOT EXISTS idx_audit_origin ON audit_log(origin_region);
CREATE INDEX IF NOT EXISTS idx_audit_entity_region ON audit_log(entity_type, entity_id, region);

-- +goose Down
--
-- DROP INDEX IF EXISTS idx_audit_entity_region;
-- DROP INDEX IF EXISTS idx_audit_origin;
-- DROP INDEX IF EXISTS idx_audit_region;
-- ALTER TABLE audit_log DROP COLUMN IF EXISTS origin_region;
-- ALTER TABLE audit_log DROP COLUMN IF EXISTS region;
