DROP INDEX IF EXISTS idx_integration_outbox_pending;

CREATE INDEX IF NOT EXISTS idx_integration_outbox_pending
    ON integration_outbox (created_at)
    WHERE published_at IS NULL;

ALTER TABLE integration_outbox
    DROP COLUMN IF EXISTS available_at;
