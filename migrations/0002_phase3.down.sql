DROP INDEX IF EXISTS decisions_event_idx;
DROP INDEX IF EXISTS decisions_decision_decided_idx;
DROP INDEX IF EXISTS decisions_account_decided_idx;

ALTER TABLE decisions DROP COLUMN IF EXISTS shadow_hits;

ALTER TABLE list_entries DROP COLUMN IF EXISTS created_at;
ALTER TABLE list_entries DROP COLUMN IF EXISTS created_by;
