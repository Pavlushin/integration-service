package engine

import (
	"context"
	"encoding/json"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/outbox"
)

type StartHeavyInput struct {
	Source         string
	CorrelationID  string
	Type           string
	Direction      enginejob.Direction
	DedupeKey      string
	IdempotencyKey string
	PayloadJSON    json.RawMessage
}

type StartHeavyResult struct {
	Job    enginejob.Job
	Reused bool
}

type DeliveryJobInput struct {
	ParentJob      enginejob.Job
	Direction      enginejob.Direction
	PayloadJSON    json.RawMessage
	IdempotencyKey string
}

type JobRepository interface {
	Create(ctx context.Context, job enginejob.Job) error
	GetByID(ctx context.Context, id string) (enginejob.Job, error)
	FindLatestByDedupeKey(ctx context.Context, dedupeKey string) (enginejob.Job, bool, error)
	UpdateStatus(ctx context.Context, jobID string, status enginejob.Status, lastError string) error
	UpdateResult(ctx context.Context, jobID string, resultPath string) error
	IncrementAttempts(ctx context.Context, jobID string, lastError string) error
}

type InboxRepository interface {
	SaveResponse(ctx context.Context, source string, idempotencyKey string, responseJSON json.RawMessage) error
	GetResponse(ctx context.Context, source string, idempotencyKey string) (json.RawMessage, bool, error)
}

type OutboxWriter interface {
	Enqueue(ctx context.Context, record outbox.Record) error
}

type IDGenerator interface {
	NewID() string
}
