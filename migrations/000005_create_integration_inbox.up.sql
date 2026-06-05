CREATE TABLE IF NOT EXISTS integration_inbox (
    source TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    response_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_integration_inbox_created_at
    ON integration_inbox (created_at);
