ALTER TABLE integration_outbox
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;

UPDATE integration_outbox
SET available_at = created_at
WHERE available_at IS NULL;

ALTER TABLE integration_outbox
    ALTER COLUMN available_at SET NOT NULL,
    ALTER COLUMN available_at SET DEFAULT NOW();

DROP INDEX IF EXISTS idx_integration_outbox_pending;

CREATE INDEX IF NOT EXISTS idx_integration_outbox_pending
    ON integration_outbox (available_at, created_at)
    WHERE published_at IS NULL;
