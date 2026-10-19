-- FADE initial schema (Phase 0).
-- Applied by golang-migrate (see deploy/docker-compose.yml, service "migrate").

CREATE TABLE rule_sets (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INT NOT NULL,
    content JSONB NOT NULL,
    content_hash TEXT NOT NULL,
    config JSONB NOT NULL,
    author TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE TABLE decisions (
    id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    recipient_id TEXT,
    amount BIGINT NOT NULL,
    txn_type TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('ALLOW','STEP_UP','HOLD','BLOCK')),
    risk_score SMALLINT NOT NULL,
    rule_score SMALLINT NOT NULL,
    ml_score SMALLINT,
    ml_status TEXT NOT NULL,
    rule_set_version INT NOT NULL,
    model_version TEXT,
    reasons JSONB NOT NULL,
    features JSONB NOT NULL,
    latency_us INT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, decided_at),
    UNIQUE (event_id, decided_at)
) PARTITION BY RANGE (decided_at);

CREATE TABLE decisions_default PARTITION OF decisions DEFAULT;

CREATE INDEX ON decisions (account_id, decided_at DESC);
CREATE INDEX ON decisions (decision, decided_at DESC);

CREATE TABLE cases (
    id BIGSERIAL PRIMARY KEY,
    decision_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','IN_REVIEW','RESOLVED')),
    priority SMALLINT NOT NULL,
    assignee TEXT,
    outcome TEXT CHECK (outcome IN ('FRAUD','LEGIT')),
    notes TEXT,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX ON cases (status, priority DESC, opened_at);

CREATE TABLE labels (
    event_id TEXT PRIMARY KEY,
    label TEXT NOT NULL CHECK (label IN ('FRAUD','LEGIT')),
    source TEXT NOT NULL CHECK (source IN ('GENERATOR','ANALYST','CHARGEBACK')),
    scenario TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    before JSONB,
    after JSONB,
    prev_hash TEXT,
    hash TEXT NOT NULL,
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE list_entries (
    id BIGSERIAL PRIMARY KEY,
    list_type TEXT NOT NULL CHECK (list_type IN ('ALLOW','BLOCK')),
    subject TEXT NOT NULL CHECK (subject IN ('ACCOUNT','DEVICE','MSISDN')),
    value TEXT NOT NULL,
    reason TEXT,
    expires_at TIMESTAMPTZ,
    UNIQUE (list_type, subject, value)
);

CREATE TABLE replay_jobs (
    id BIGSERIAL PRIMARY KEY,
    dataset TEXT NOT NULL,
    baseline JSONB NOT NULL,
    candidate JSONB NOT NULL,
    status TEXT NOT NULL,
    report JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
