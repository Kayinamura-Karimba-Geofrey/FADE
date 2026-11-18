-- FADE Phase 6 replay and simulation schema.
-- The Phase 0 replay_jobs table is extended in place; the old candidate/report
-- JSONB columns stay for backwards compatibility with phase-0 consumers.

-- Snapshot replay needs the decision input, not just its features, so rule
-- context (fee, tier, balance, txn type) can be reconstructed exactly.
ALTER TABLE decisions ADD COLUMN IF NOT EXISTS input_event JSONB;

ALTER TABLE replay_jobs
    ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT 'system',
    ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'full',
    ADD COLUMN IF NOT EXISTS state_policy TEXT NOT NULL DEFAULT 'ignore',
    ADD COLUMN IF NOT EXISTS dataset_hash TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS config_hashes JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS seed BIGINT NOT NULL DEFAULT 11,
    ADD COLUMN IF NOT EXISTS progress NUMERIC(5,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS error TEXT,
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS report_path TEXT,
    ADD COLUMN IF NOT EXISTS report_hash TEXT,
    ADD COLUMN IF NOT EXISTS engine_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS options JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;

ALTER TABLE replay_jobs
    ADD CONSTRAINT replay_jobs_mode_check
        CHECK (mode IN ('full', 'snapshot', 'stored-ml')) NOT VALID;
ALTER TABLE replay_jobs
    ADD CONSTRAINT replay_jobs_state_policy_check
        CHECK (state_policy IN ('ignore', 'apply-block')) NOT VALID;
ALTER TABLE replay_jobs
    ADD CONSTRAINT replay_jobs_status_check
        CHECK (status IN ('QUEUED','RUNNING','DONE','FAILED','CANCELLED')) NOT VALID;

CREATE INDEX IF NOT EXISTS replay_jobs_status_created_idx
    ON replay_jobs (status, created_at);
CREATE INDEX IF NOT EXISTS replay_jobs_created_by_idx
    ON replay_jobs (created_by, created_at DESC);
CREATE INDEX IF NOT EXISTS replay_jobs_dataset_idx
    ON replay_jobs (dataset, created_at DESC);

CREATE TABLE IF NOT EXISTS replay_results (
    job_id BIGINT NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
    config_label TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    metrics JSONB NOT NULL,
    per_scenario JSONB NOT NULL DEFAULT '[]'::jsonb,
    per_rule JSONB NOT NULL DEFAULT '[]'::jsonb,
    daily JSONB NOT NULL DEFAULT '[]'::jsonb,
    segments JSONB NOT NULL DEFAULT '[]'::jsonb,
    sweep JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, config_label)
);

CREATE TABLE IF NOT EXISTS replay_deltas (
    job_id BIGINT NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
    candidate_label TEXT NOT NULL,
    summary JSONB NOT NULL,
    top_changes JSONB NOT NULL DEFAULT '[]'::jsonb,
    transition JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, candidate_label)
);

CREATE TABLE IF NOT EXISTS replay_gates (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
    candidate_label TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    config_hash TEXT NOT NULL,
    rules_hash TEXT NOT NULL,
    model_version TEXT NOT NULL DEFAULT '',
    checks JSONB NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS replay_gates_artifact_idx
    ON replay_gates (rules_hash, model_version, created_at DESC);

-- REPLAY_START, REPLAY_FINISH and REPLAY_PROMOTION_GATE are added to the
-- code-level audit catalog in internal/audit/audit.go; the catalog is not a
-- database table. Analyst/auditor access is inherited from the 0003 role model.
