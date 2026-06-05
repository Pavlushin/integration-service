package engine

import (
	"context"
	"encoding/json"

	"onec-integration/internal/outbox"
)

type NoopInboxRepository struct{}

func (NoopInboxRepository) SaveResponse(_ context.Context, _ string, _ string, _ json.RawMessage) error {
	return nil
}

func (NoopInboxRepository) GetResponse(_ context.Context, _ string, _ string) (json.RawMessage, bool, error) {
	return nil, false, nil
}

type NoopOutboxWriter struct{}

func (NoopOutboxWriter) Enqueue(_ context.Context, _ outbox.Record) error {
	return nil
}
