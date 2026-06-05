CREATE TABLE IF NOT EXISTS integration_jobs (
    id TEXT PRIMARY KEY,
    correlation_id TEXT NOT NULL,
    parent_id TEXT,
    type TEXT NOT NULL,
    kind TEXT NOT NULL,
    direction TEXT NOT NULL,
    status TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    idempotency_key TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_path TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_integration_jobs_correlation_id
    ON integration_jobs (correlation_id);

CREATE INDEX IF NOT EXISTS idx_integration_jobs_parent_id
    ON integration_jobs (parent_id);

CREATE INDEX IF NOT EXISTS idx_integration_jobs_status
    ON integration_jobs (status);

CREATE INDEX IF NOT EXISTS idx_integration_jobs_type_kind
    ON integration_jobs (type, kind);

CREATE INDEX IF NOT EXISTS idx_integration_jobs_dedupe_key_active
    ON integration_jobs (dedupe_key)
    WHERE status IN ('received', 'validated', 'prepared', 'delivering', 'retrying');
