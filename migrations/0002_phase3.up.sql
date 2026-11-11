-- FADE Phase 3 additive schema.
-- The decision API applies the same statements defensively at startup
-- (pipeline.PostgresStore.EnsureSchema), so both paths stay compatible.

ALTER TABLE list_entries ADD COLUMN IF NOT EXISTS created_by TEXT;
ALTER TABLE list_entries ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS shadow_hits JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS decisions_account_decided_idx ON decisions (account_id, decided_at DESC);
CREATE INDEX IF NOT EXISTS decisions_decision_decided_idx ON decisions (decision, decided_at DESC);
CREATE INDEX IF NOT EXISTS decisions_event_idx ON decisions (event_id);
