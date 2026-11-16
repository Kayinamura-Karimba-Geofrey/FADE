-- Reverse of 0003_explain_audit. Destructive by nature: it removes Phase 5
-- provenance, reproduction records and checkpoint history. Audit rows are never
-- deleted by this migration; only the additive Phase 5 columns and objects are
-- removed. The append-only trigger is dropped last so the columns can be
-- altered.

BEGIN;

DROP FUNCTION IF EXISTS ensure_decisions_partition(TIMESTAMPTZ);
DROP TABLE IF EXISTS retention_policy;

DROP TRIGGER IF EXISTS audit_log_no_truncate ON audit_log;
DROP TRIGGER IF EXISTS audit_log_no_update ON audit_log;
DROP FUNCTION IF EXISTS audit_log_forbid_mutation();

DROP TABLE IF EXISTS audit_checkpoints;
DROP INDEX IF EXISTS audit_log_hash_uidx;
DROP INDEX IF EXISTS audit_log_seq_uidx;
ALTER TABLE audit_log DROP COLUMN IF EXISTS source_ip_hash;
ALTER TABLE audit_log DROP COLUMN IF EXISTS request_id;
ALTER TABLE audit_log DROP COLUMN IF EXISTS seq;

DROP TABLE IF EXISTS explain_templates_version;
DROP TABLE IF EXISTS decision_reproductions;

DROP INDEX IF EXISTS decisions_provenance_gin;
DROP INDEX IF EXISTS decisions_ruleset_idx;
DROP INDEX IF EXISTS decisions_snapshot_idx;
ALTER TABLE decisions DROP COLUMN IF EXISTS ruleset_hash;
ALTER TABLE decisions DROP COLUMN IF EXISTS explain_schema_version;
ALTER TABLE decisions DROP COLUMN IF EXISTS provenance;
ALTER TABLE decisions DROP COLUMN IF EXISTS snapshot_id;

COMMIT;
