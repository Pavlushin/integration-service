package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/outbox"
)

const (
	defaultDLQLimit = 20
	maxDLQLimit     = 100
)

type DLQJobRepository interface {
	GetByID(ctx context.Context, id string) (enginejob.Job, error)
	ListByStatus(ctx context.Context, status enginejob.Status, limit int) ([]enginejob.Job, error)
	ResetForRetry(ctx context.Context, jobID string) error
}

type DLQService struct {
	jobs   DLQJobRepository
	outbox OutboxWriter
	ids    IDGenerator
}

func NewDLQService(jobs DLQJobRepository, outboxWriter OutboxWriter, ids IDGenerator) (*DLQService, error) {
	switch {
	case jobs == nil:
		return nil, fmt.Errorf("dlq job repository is required")
	case outboxWriter == nil:
		return nil, fmt.Errorf("outbox writer is required")
	case ids == nil:
		return nil, fmt.Errorf("id generator is required")
	}

	return &DLQService{
		jobs:   jobs,
		outbox: outboxWriter,
		ids:    ids,
	}, nil
}

func (s *DLQService) ListDLQJobs(ctx context.Context, limit int) ([]enginejob.Job, error) {
	if s == nil {
		return nil, fmt.Errorf("dlq service is nil")
	}
	if limit <= 0 {
		limit = defaultDLQLimit
	}
	if limit > maxDLQLimit {
		limit = maxDLQLimit
	}

	jobs, err := s.jobs.ListByStatus(ctx, enginejob.StatusDLQ, limit)
	if err != nil {
		return nil, fmt.Errorf("list dlq jobs: %w", err)
	}

	return jobs, nil
}

func (s *DLQService) RequeueDLQJob(ctx context.Context, jobID string) (enginejob.Job, error) {
	if s == nil {
		return enginejob.Job{}, fmt.Errorf("dlq service is nil")
	}
	if jobID == "" {
		return enginejob.Job{}, fmt.Errorf("job id is required")
	}

	job, err := s.jobs.GetByID(ctx, jobID)
	if err != nil {
		return enginejob.Job{}, fmt.Errorf("load dlq job: %w", err)
	}
	if job.Status != enginejob.StatusDLQ {
		return enginejob.Job{}, fmt.Errorf("job is not in dlq status: %s", job.Status)
	}

	topic, err := topicForJobKind(job.Kind)
	if err != nil {
		return enginejob.Job{}, err
	}

	if err := s.jobs.ResetForRetry(ctx, job.ID); err != nil {
		return enginejob.Job{}, fmt.Errorf("reset dlq job for retry: %w", err)
	}

	payload, err := json.Marshal(map[string]string{"job_id": job.ID})
	if err != nil {
		return enginejob.Job{}, fmt.Errorf("marshal dlq requeue payload: %w", err)
	}

	now := time.Now().UTC()
	record := outbox.Record{
		ID:          s.ids.NewID(),
		Topic:       topic,
		PayloadJSON: payload,
		CreatedAt:   now,
		AvailableAt: now,
	}
	if err := s.outbox.Enqueue(ctx, record); err != nil {
		return enginejob.Job{}, fmt.Errorf("enqueue dlq requeue outbox record: %w", err)
	}

	job.Status = enginejob.StatusRetrying
	job.Attempts = 0
	job.LastError = ""
	return job, nil
}

func topicForJobKind(kind enginejob.Kind) (string, error) {
	switch kind {
	case enginejob.KindPrepare:
		return PrepareTopic, nil
	case enginejob.KindDelivery:
		return DeliveryTopic, nil
	default:
		return "", fmt.Errorf("unsupported job kind for requeue: %s", kind)
	}
}
