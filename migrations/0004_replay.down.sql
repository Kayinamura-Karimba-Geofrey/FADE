ALTER TABLE decisions DROP COLUMN IF EXISTS input_event;

DROP TABLE IF EXISTS replay_gates;
DROP TABLE IF EXISTS replay_deltas;
DROP TABLE IF EXISTS replay_results;

DROP INDEX IF EXISTS replay_jobs_dataset_idx;
DROP INDEX IF EXISTS replay_jobs_created_by_idx;
DROP INDEX IF EXISTS replay_jobs_status_created_idx;

ALTER TABLE replay_jobs DROP CONSTRAINT IF EXISTS replay_jobs_status_check;
ALTER TABLE replay_jobs DROP CONSTRAINT IF EXISTS replay_jobs_state_policy_check;
ALTER TABLE replay_jobs DROP CONSTRAINT IF EXISTS replay_jobs_mode_check;

ALTER TABLE replay_jobs
    DROP COLUMN IF EXISTS name,
    DROP COLUMN IF EXISTS created_by,
    DROP COLUMN IF EXISTS mode,
    DROP COLUMN IF EXISTS state_policy,
    DROP COLUMN IF EXISTS dataset_hash,
    DROP COLUMN IF EXISTS config_hashes,
    DROP COLUMN IF EXISTS seed,
    DROP COLUMN IF EXISTS progress,
    DROP COLUMN IF EXISTS error,
    DROP COLUMN IF EXISTS started_at,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS report_path,
    DROP COLUMN IF EXISTS report_hash,
    DROP COLUMN IF EXISTS engine_version,
    DROP COLUMN IF EXISTS options,
    DROP COLUMN IF EXISTS cancelled_at;
