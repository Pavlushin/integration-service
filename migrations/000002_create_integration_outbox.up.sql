CREATE TABLE IF NOT EXISTS integration_outbox (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_integration_outbox_pending
    ON integration_outbox (created_at)
    WHERE published_at IS NULL;
