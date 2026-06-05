package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	"onec-integration/internal/outbox"
	"onec-integration/internal/storage"
	"onec-integration/internal/workflow"

	"go.uber.org/zap"
)

type Service struct {
	jobs      JobRepository
	inbox     InboxRepository
	storage   storage.Storage
	workflows *workflow.Registry
	ids       IDGenerator
}

func NewService(
	jobs JobRepository,
	inbox InboxRepository,
	storageProvider storage.Storage,
	workflows *workflow.Registry,
	ids IDGenerator,
) (*Service, error) {
	switch {
	case jobs == nil:
		return nil, fmt.Errorf("job repository is required")
	case inbox == nil:
		return nil, fmt.Errorf("inbox repository is required")
	case storageProvider == nil:
		return nil, fmt.Errorf("storage is required")
	case workflows == nil:
		return nil, fmt.Errorf("workflow registry is required")
	case ids == nil:
		return nil, fmt.Errorf("id generator is required")
	}

	return &Service{
		jobs:      jobs,
		inbox:     inbox,
		storage:   storageProvider,
		workflows: workflows,
		ids:       ids,
	}, nil
}

func (s *Service) StartHeavy(ctx context.Context, input StartHeavyInput) (StartHeavyResult, error) {
	if s == nil {
		return StartHeavyResult{}, fmt.Errorf("engine service is nil")
	}

	log := logger.FromContext(ctx).With(
		zap.String("job_type", input.Type),
		zap.String("direction", string(input.Direction)),
		zap.String("dedupe_key", input.DedupeKey),
	)

	if input.Type == "" {
		return StartHeavyResult{}, fmt.Errorf("job type is required")
	}
	if input.Direction == "" {
		return StartHeavyResult{}, fmt.Errorf("job direction is required")
	}
	if input.DedupeKey == "" {
		return StartHeavyResult{}, fmt.Errorf("dedupe key is required")
	}

	if previousJob, found, err := s.jobs.FindLatestByDedupeKey(ctx, input.DedupeKey); err != nil {
		return StartHeavyResult{}, fmt.Errorf("find latest job by dedupe key: %w", err)
	} else if found && isReusableDedupeJob(previousJob) {
		log.Info(
			"job reused by dedupe key",
			zap.String("job_id", previousJob.ID),
			zap.String("status", string(previousJob.Status)),
		)
		return StartHeavyResult{
			Job:    previousJob,
			Reused: true,
		}, nil
	} else if found {
		log.Info(
			"terminal unsuccessful job is not reused by dedupe key",
			zap.String("job_id", previousJob.ID),
			zap.String("status", string(previousJob.Status)),
		)
	}

	correlationID := input.CorrelationID
	if correlationID == "" {
		correlationID = s.ids.NewID()
	}

	now := time.Now().UTC()
	newJob := enginejob.Job{
		ID:             s.ids.NewID(),
		CorrelationID:  correlationID,
		Type:           input.Type,
		Kind:           enginejob.KindPrepare,
		Direction:      input.Direction,
		Status:         enginejob.StatusReceived,
		DedupeKey:      input.DedupeKey,
		IdempotencyKey: input.IdempotencyKey,
		PayloadJSON:    input.PayloadJSON,
		Attempts:       0,
		CreatedAt:      now,
	}

	messagePayload, err := json.Marshal(map[string]string{
		"job_id": newJob.ID,
	})
	if err != nil {
		return StartHeavyResult{}, fmt.Errorf("marshal outbox payload: %w", err)
	}

	outboxRecord := outbox.Record{
		ID:          s.ids.NewID(),
		Topic:       PrepareTopic,
		PayloadJSON: messagePayload,
		CreatedAt:   now,
	}
	if err := s.jobs.CreateWithOutbox(ctx, newJob, outboxRecord); err != nil {
		return StartHeavyResult{}, fmt.Errorf("create prepare job and outbox record: %w", err)
	}
	log.Info(
		"prepare job and outbox record created",
		zap.String("job_id", newJob.ID),
		zap.String("status", string(newJob.Status)),
		zap.String("topic", outboxRecord.Topic),
		zap.String("outbox_id", outboxRecord.ID),
	)

	return StartHeavyResult{
		Job:    newJob,
		Reused: false,
	}, nil
}

func isReusableDedupeJob(job enginejob.Job) bool {
	return job.IsActive() || job.Status == enginejob.StatusDone
}
