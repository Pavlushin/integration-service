CREATE TABLE IF NOT EXISTS integration_job_audit (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES integration_jobs(id),
    action TEXT NOT NULL,
    actor TEXT,
    reason TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_integration_job_audit_job_id_created_at
    ON integration_job_audit (job_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_integration_job_audit_action_created_at
    ON integration_job_audit (action, created_at DESC);
