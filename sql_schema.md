~~~~sql
-- =========================================================
-- Fabric Data Portal - Metadata & Logging Schema
-- =========================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =========================================================
-- 1. pipeline_runs
-- One row = one Fabric pipeline execution
-- =========================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,

    triggered_by             TEXT NOT NULL,
    input_file               TEXT NOT NULL,

    workspace_id             TEXT NOT NULL,
    pipeline_item_id         TEXT NOT NULL,

    fabric_job_location_url  TEXT,
    fabric_job_id            TEXT,

    status                   TEXT NOT NULL CHECK (
        status IN (
            'SUBMITTED',
            'QUEUED',
            'RUNNING',
            'SUCCEEDED',
            'FAILED'
        )
    ),

    error_message            TEXT,

    -- KPI snapshot produced by the notebook (kpis.json)
    kpis                     JSONB,

    -- Optional technical metadata
    notebook_version         TEXT,
    app_version              TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_created_at
    ON pipeline_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs (status);

-- =========================================================
-- 2. run_lock
-- Prevents overlapping pipeline executions
-- =========================================================

CREATE TABLE IF NOT EXISTS run_lock (
    lock_key     TEXT PRIMARY KEY,
    run_id       UUID,
    locked_at    TIMESTAMP,
    locked_by    TEXT
);

-- Insert single global lock row (idempotent)
INSERT INTO run_lock (lock_key)
VALUES ('ACTIVE_PIPELINE_RUN')
ON CONFLICT DO NOTHING;

-- =========================================================
-- 3. run_artifacts
-- Tracks which files were produced by which run
-- =========================================================

CREATE TABLE IF NOT EXISTS run_artifacts (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id        UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,

    artifact_type TEXT NOT NULL,         -- parquet | csv | json | profile
    file_path     TEXT NOT NULL,         -- Fabric OneLake path
    file_size     BIGINT,

    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_run_artifacts_run_id
    ON run_artifacts (run_id);

-- =========================================================
-- 4. run_events
-- Timeline of status changes, logs, warnings, errors
-- =========================================================

CREATE TABLE IF NOT EXISTS run_events (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,

    event_time  TIMESTAMP NOT NULL DEFAULT NOW(),
    event_type  TEXT NOT NULL,    -- STATUS_CHANGE | LOG | WARNING | ERROR
    message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_events_run_id
    ON run_events (run_id);

-- =========================================================
-- 5. run_restores
-- Tracks restoring historical runs to "current"
-- =========================================================

CREATE TABLE IF NOT EXISTS run_restores (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    restored_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    restored_by     TEXT NOT NULL,

    source_run_id   UUID NOT NULL REFERENCES pipeline_runs(run_id),
    target_run_id   UUID
);

-- =========================================================
-- END OF SCHEMA
-- =========================================================
~~~~