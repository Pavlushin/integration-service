package outbox

import "context"

type Writer interface {
	Enqueue(ctx context.Context, record Record) error
}

type Reader interface {
	ListPending(ctx context.Context, limit int) ([]Record, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, lastError string) error
}
