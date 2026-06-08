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
	"onec-integration/internal/telemetry"
	"onec-integration/internal/workflow"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
	ctx, span := telemetry.Tracer().Start(ctx, "engine.start_heavy")
	defer span.End()

	log := logger.FromContext(ctx).With(
		zap.String("job_type", input.Type),
		zap.String("direction", string(input.Direction)),
		zap.String("dedupe_key", input.DedupeKey),
	)
	span.SetAttributes(
		attribute.String("job.type", input.Type),
		attribute.String("job.direction", string(input.Direction)),
		attribute.String("job.dedupe_key", input.DedupeKey),
	)

	if input.Type == "" {
		span.SetStatus(codes.Error, "job type is required")
		return StartHeavyResult{}, fmt.Errorf("job type is required")
	}
	if input.Direction == "" {
		span.SetStatus(codes.Error, "job direction is required")
		return StartHeavyResult{}, fmt.Errorf("job direction is required")
	}
	if input.DedupeKey == "" {
		span.SetStatus(codes.Error, "dedupe key is required")
		return StartHeavyResult{}, fmt.Errorf("dedupe key is required")
	}
	if input.IdempotencyKey != "" && input.Source == "" {
		span.SetStatus(codes.Error, "source is required when idempotency key is provided")
		return StartHeavyResult{}, fmt.Errorf("source is required when idempotency key is provided")
	}

	if input.IdempotencyKey != "" {
		if responseJSON, found, err := s.inbox.GetResponse(ctx, input.Source, input.IdempotencyKey); err != nil {
			return StartHeavyResult{}, fmt.Errorf("get idempotency response: %w", err)
		} else if found {
			result, err := startHeavyResultFromInboxResponse(responseJSON)
			if err != nil {
				return StartHeavyResult{}, err
			}
			log.Info(
				"job reused by idempotency key",
				zap.String("job_id", result.Job.ID),
				zap.String("idempotency_key", input.IdempotencyKey),
			)
			return result, nil
		}
	}

	if previousJob, found, err := s.jobs.FindLatestByDedupeKey(ctx, input.DedupeKey); err != nil {
		return StartHeavyResult{}, fmt.Errorf("find latest job by dedupe key: %w", err)
	} else if found && isReusableDedupeJob(previousJob) {
		if input.IdempotencyKey != "" {
			responseJSON, err := marshalStartHeavyIdempotencyResponse(previousJob)
			if err != nil {
				return StartHeavyResult{}, err
			}
			if err := s.inbox.SaveResponse(ctx, input.Source, input.IdempotencyKey, responseJSON); err != nil {
				return StartHeavyResult{}, fmt.Errorf("save idempotency response: %w", err)
			}
		}
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
			"terminal job is not reused by dedupe key",
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
		Headers:     telemetry.InjectHeaders(ctx),
		CreatedAt:   now,
	}
	if input.IdempotencyKey != "" {
		responseJSON, err := marshalStartHeavyIdempotencyResponse(newJob)
		if err != nil {
			return StartHeavyResult{}, err
		}
		if err := s.jobs.CreateWithOutboxAndInbox(ctx, newJob, outboxRecord, input.Source, input.IdempotencyKey, responseJSON); err != nil {
			if reusedResult, found, reuseErr := s.reuseIdempotencyResponse(ctx, input.Source, input.IdempotencyKey); reuseErr != nil {
				return StartHeavyResult{}, reuseErr
			} else if found {
				return reusedResult, nil
			}
			return StartHeavyResult{}, fmt.Errorf("create prepare job, outbox record and idempotency response: %w", err)
		}
	} else if err := s.jobs.CreateWithOutbox(ctx, newJob, outboxRecord); err != nil {
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
	return job.IsActive()
}

func marshalStartHeavyIdempotencyResponse(job enginejob.Job) (json.RawMessage, error) {
	responseJSON, err := json.Marshal(startHeavyIdempotencyResponse{
		JobID:         job.ID,
		CorrelationID: job.CorrelationID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal idempotency response: %w", err)
	}
	return responseJSON, nil
}

func startHeavyResultFromInboxResponse(responseJSON json.RawMessage) (StartHeavyResult, error) {
	var response startHeavyIdempotencyResponse
	if err := json.Unmarshal(responseJSON, &response); err != nil {
		return StartHeavyResult{}, fmt.Errorf("decode idempotency response: %w", err)
	}
	if response.JobID == "" {
		return StartHeavyResult{}, fmt.Errorf("idempotency response job_id is required")
	}
	return StartHeavyResult{
		Job: enginejob.Job{
			ID:            response.JobID,
			CorrelationID: response.CorrelationID,
		},
		Reused: true,
	}, nil
}

func (s *Service) reuseIdempotencyResponse(ctx context.Context, source string, idempotencyKey string) (StartHeavyResult, bool, error) {
	responseJSON, found, err := s.inbox.GetResponse(ctx, source, idempotencyKey)
	if err != nil {
		return StartHeavyResult{}, false, fmt.Errorf("get idempotency response after create failure: %w", err)
	}
	if !found {
		return StartHeavyResult{}, false, nil
	}
	result, err := startHeavyResultFromInboxResponse(responseJSON)
	if err != nil {
		return StartHeavyResult{}, false, err
	}
	return result, true, nil
}
