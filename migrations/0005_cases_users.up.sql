-- FADE Phase 7 case management and analyst dashboard schema.

-- Extend cases table for state machine, SLA, priority, versioning, four-eyes.
ALTER TABLE cases ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'MANUAL'
  CHECK (source IN ('DECISION','STEP_UP_FAILED','QA_SAMPLE','MANUAL'));
ALTER TABLE cases ADD COLUMN IF NOT EXISTS decision_class TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS primary_decision_id TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS linked_decision_ids TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE cases ADD COLUMN IF NOT EXISTS risk_score SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS amount_total BIGINT NOT NULL DEFAULT 0;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS sla_due_at TIMESTAMPTZ;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS sla_paused_at TIMESTAMPTZ;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS escalated BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS reason_code TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS outcome_confidence TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS pending_approval BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS approved_by TEXT;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS follow_up_at TIMESTAMPTZ;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS second_review_of BIGINT REFERENCES cases(id);
ALTER TABLE cases ADD COLUMN IF NOT EXISTS repeat_account BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE cases ADD COLUMN IF NOT EXISTS account_id_hash TEXT;
-- Persisted so a four-eyes approval labels the same scope the analyst chose.
ALTER TABLE cases ADD COLUMN IF NOT EXISTS propagate_linked BOOLEAN NOT NULL DEFAULT FALSE;

-- Update status check to include all states.
ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_status_check;
ALTER TABLE cases ADD CONSTRAINT cases_status_check
  CHECK (status IN ('OPEN','IN_REVIEW','ESCALATED','RESOLVED','REOPENED'));

-- INCONCLUSIVE is a valid analyst verdict (it writes no label).
ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_outcome_check;
ALTER TABLE cases ADD CONSTRAINT cases_outcome_check
  CHECK (outcome IS NULL OR outcome IN ('FRAUD','LEGIT','INCONCLUSIVE'));

-- The base table requires a decision id; manual cases still carry a synthetic one.
ALTER TABLE cases ALTER COLUMN decision_id SET DEFAULT '';
ALTER TABLE cases ALTER COLUMN outcome DROP NOT NULL;

-- Indexes.
CREATE INDEX IF NOT EXISTS cases_status_priority_idx ON cases (status, priority DESC, opened_at);
CREATE INDEX IF NOT EXISTS cases_assignee_status_idx ON cases (assignee, status);
CREATE INDEX IF NOT EXISTS cases_account_opened_idx ON cases (account_id, opened_at DESC);
CREATE INDEX IF NOT EXISTS cases_sla_due_idx ON cases (sla_due_at) WHERE status IN ('OPEN','IN_REVIEW','ESCALATED');

-- case_events append-only timeline.
CREATE TABLE IF NOT EXISTS case_events (
    id BIGSERIAL PRIMARY KEY,
    case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    at TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor TEXT NOT NULL,
    actor_role TEXT NOT NULL,
    type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS case_events_case_at_idx ON case_events (case_id, at);

-- Propagation writes a label for each linked decision; its own source keeps it
-- distinguishable from a human verdict during evaluation.
ALTER TABLE labels DROP CONSTRAINT IF EXISTS labels_source_check;
ALTER TABLE labels ADD CONSTRAINT labels_source_check
  CHECK (source IN ('GENERATOR','ANALYST','CHARGEBACK','PROPAGATED'));

-- label_history append-only.
CREATE TABLE IF NOT EXISTS label_history (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    label TEXT NOT NULL CHECK (label IN ('FRAUD','LEGIT','INCONCLUSIVE')),
    source TEXT NOT NULL,
    case_id BIGINT REFERENCES cases(id),
    at TIMESTAMPTZ NOT NULL DEFAULT now(),
    superseded_by BIGINT REFERENCES label_history(id),
    superseded_at TIMESTAMPTZ,
    mature_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS label_history_event_idx ON label_history (event_id, at DESC);
CREATE INDEX IF NOT EXISTS label_history_case_idx ON label_history (case_id, at DESC);
CREATE INDEX IF NOT EXISTS label_history_mature_idx ON label_history (mature_at) WHERE superseded_by IS NULL;

-- users.
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('service','analyst','lead','admin','auditor')),
    password_hash TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    failed_attempts INT NOT NULL DEFAULT 0,
    locked_until TIMESTAMPTZ,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    mfa_secret TEXT
);

-- sessions/refresh tokens.
CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    user_agent_hash TEXT,
    ip_hash TEXT
);
CREATE INDEX IF NOT EXISTS sessions_user_idx ON sessions (user_id, created_at DESC);

-- saved filters.
CREATE TABLE IF NOT EXISTS saved_filters (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, name)
);

-- analyst presence.
CREATE TABLE IF NOT EXISTS analyst_presence (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL CHECK (status IN ('ONLINE','AWAY'))
);

-- second reviews.
CREATE TABLE IF NOT EXISTS second_reviews (
    id BIGSERIAL PRIMARY KEY,
    case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    reviewer TEXT NOT NULL,
    outcome TEXT NOT NULL,
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- outbox.
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;

-- idempotency records for mutating case actions.
CREATE TABLE IF NOT EXISTS case_idempotency (
    key TEXT PRIMARY KEY,
    case_id BIGINT NOT NULL,
    action TEXT NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS case_idempotency_case_idx ON case_idempotency (case_id, created_at DESC);

-- config tables.
CREATE TABLE IF NOT EXISTS case_config (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL
);

-- revoke update/delete on append-only tables from app role.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fade_app') THEN
    REVOKE UPDATE, DELETE ON case_events, label_history, audit_log FROM fade_app;
  END IF;
END $$;
