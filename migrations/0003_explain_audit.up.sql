-- FADE Phase 5: explainability + tamper-evident audit.
--
-- This is migration 0003 (not 0002 as the Phase 5 specification says) because
-- 0002_phase3 already occupies that number. See docs/audit.md "Deviations".
--
-- The migration is additive: no Phase 3/4 column is renamed or dropped, and a
-- decision row remains valid without provenance (older Phase 3/4 rows).

BEGIN;

-- ---------------------------------------------------------------- explanations
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS snapshot_id TEXT;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS provenance JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS explain_schema_version INT NOT NULL DEFAULT 1;
-- ruleset_hash is stored separately from provenance so a reviewer can index it
-- without a JSONB scan.
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS ruleset_hash TEXT;

CREATE INDEX IF NOT EXISTS decisions_snapshot_idx ON decisions (snapshot_id);
CREATE INDEX IF NOT EXISTS decisions_ruleset_idx ON decisions (rule_set_version, decided_at DESC);
CREATE INDEX IF NOT EXISTS decisions_provenance_gin ON decisions USING GIN (provenance);

-- Every reproduction attempt is recorded. Repro must be auditable too: a
-- mismatch is evidence, not a transient CLI log line.
CREATE TABLE IF NOT EXISTS decision_reproductions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    decision_id TEXT NOT NULL,
    snapshot_id TEXT,
    expected_hash TEXT NOT NULL,
    actual_hash TEXT NOT NULL,
    match BOOLEAN NOT NULL,
    diffs JSONB NOT NULL DEFAULT '[]'::jsonb,
    tool_version TEXT NOT NULL,
    actor TEXT NOT NULL,
    reproduced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS decision_reproductions_decision_idx
    ON decision_reproductions (decision_id, reproduced_at DESC);

-- Which template catalog version rendered a decision. Content hash makes the
-- exact sentences recoverable without storing rendered text per decision.
CREATE TABLE IF NOT EXISTS explain_templates_version (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    language TEXT NOT NULL,
    version INT NOT NULL,
    content_hash TEXT NOT NULL,
    needs_review BOOLEAN NOT NULL DEFAULT true,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (language, version)
);

-- --------------------------------------------------------------------- audit
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS seq BIGINT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS request_id TEXT;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS source_ip_hash TEXT;

-- Legacy rows written before Phase 5 have no seq. They remain readable and are
-- surfaced as a legacy prelude by the verifier; new rows start at max(seq)+1.
CREATE UNIQUE INDEX IF NOT EXISTS audit_log_seq_uidx ON audit_log (seq) WHERE seq IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS audit_log_hash_uidx ON audit_log (hash);

CREATE TABLE IF NOT EXISTS audit_checkpoints (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schema_version INT NOT NULL,
    chain TEXT NOT NULL,
    seq BIGINT NOT NULL,
    hash TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    digest TEXT NOT NULL,
    path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_checkpoints_seq_idx ON audit_checkpoints (seq DESC);
CREATE INDEX IF NOT EXISTS audit_checkpoints_created_idx ON audit_checkpoints (created_at DESC);

-- Append-only enforcement. The application role is also denied UPDATE/DELETE/
-- TRUNCATE below; the trigger is defence in depth against a mis-granted role.
CREATE OR REPLACE FUNCTION audit_log_forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only (attempted %)', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

DROP TRIGGER IF EXISTS audit_log_no_update ON audit_log;
CREATE TRIGGER audit_log_no_update
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION audit_log_forbid_mutation();

DROP TRIGGER IF EXISTS audit_log_no_truncate ON audit_log;
CREATE TRIGGER audit_log_no_truncate
    BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION audit_log_forbid_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON audit_log FROM PUBLIC;

-- Grant INSERT/SELECT only to the application role when it exists. Role names
-- are deployment-specific, so this is conditional and idempotent.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fade_app') THEN
        GRANT SELECT, INSERT ON audit_log TO fade_app;
        GRANT SELECT, INSERT ON audit_checkpoints TO fade_app;
        GRANT USAGE, SELECT ON SEQUENCE audit_log_id_seq TO fade_app;
        GRANT USAGE, SELECT ON SEQUENCE audit_checkpoints_id_seq TO fade_app;
    END IF;
END;
$$;

-- ----------------------------------------------------------------- retention
CREATE TABLE IF NOT EXISTS retention_policy (
    table_name TEXT PRIMARY KEY,
    retain_days INT NOT NULL CHECK (retain_days >= 1),
    months_ahead INT NOT NULL DEFAULT 3 CHECK (months_ahead >= 1),
    partition_interval TEXT NOT NULL DEFAULT 'monthly'
            CHECK (partition_interval IN ('monthly')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO retention_policy (table_name, retain_days, months_ahead)
VALUES ('decisions', 90, 3)
ON CONFLICT (table_name) DO NOTHING;

-- Create a monthly partition on demand. The job calls this for the current and
-- next three months; the default partition keeps un-migrated writers working.
CREATE OR REPLACE FUNCTION ensure_decisions_partition(ts TIMESTAMPTZ)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    month_start DATE := date_trunc('month', ts AT TIME ZONE 'UTC')::date;
    next_start DATE := (month_start + INTERVAL '1 month')::date;
    part_name TEXT := 'decisions_' || to_char(month_start, 'YYYYMM');
BEGIN
    IF to_regclass(part_name) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF decisions FOR VALUES FROM (%L) TO (%L)',
            part_name, month_start, next_start);
    END IF;
    RETURN part_name;
END;
$$;

COMMIT;
