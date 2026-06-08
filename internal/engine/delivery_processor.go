package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	"onec-integration/internal/storage"
	"onec-integration/internal/telemetry"
	"onec-integration/internal/worksheetsexport"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

type DeliveryProcessor struct {
	jobs    JobRepository
	outbox  OutboxWriter
	ids     IDGenerator
	storage storage.Storage
	retry   RetryPolicy
}

func NewDeliveryProcessor(
	jobs JobRepository,
	outboxWriter OutboxWriter,
	ids IDGenerator,
	storageProvider storage.Storage,
	retryPolicy RetryPolicy,
) (*DeliveryProcessor, error) {
	switch {
	case jobs == nil:
		return nil, fmt.Errorf("job repository is required")
	case outboxWriter == nil:
		return nil, fmt.Errorf("outbox writer is required")
	case ids == nil:
		return nil, fmt.Errorf("id generator is required")
	case storageProvider == nil:
		return nil, fmt.Errorf("storage is required")
	}

	return &DeliveryProcessor{
		jobs:    jobs,
		outbox:  outboxWriter,
		ids:     ids,
		storage: storageProvider,
		retry:   retryPolicy.WithDefaults(),
	}, nil
}

func (p *DeliveryProcessor) Process(ctx context.Context, jobID string) error {
	if p == nil {
		return fmt.Errorf("delivery processor is nil")
	}
	ctx, span := telemetry.Tracer().Start(ctx, "delivery.process")
	defer span.End()
	span.SetAttributes(attribute.String("job.id", jobID))

	log := logger.FromContext(ctx).With(zap.String("job_id", jobID), zap.String("stage", "delivery"))
	log.Info("delivery processing started")
	startedAt := time.Now()
	defer func() {
		log.Info("delivery processing finished", zap.Duration("latency", time.Since(startedAt)))
	}()

	job, err := p.jobs.GetByID(ctx, jobID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "load delivery job")
		return fmt.Errorf("load delivery job: %w", err)
	}
	log = log.With(
		zap.String("correlation_id", job.CorrelationID),
		zap.String("job_type", job.Type),
	)
	span.SetAttributes(
		attribute.String("job.correlation_id", job.CorrelationID),
		attribute.String("job.type", job.Type),
	)

	if job.Kind != enginejob.KindDelivery {
		return p.recordFailure(ctx, log, job, fmt.Errorf("unexpected job kind: %s", job.Kind))
	}

	if job.IsTerminal() {
		return nil
	}

	if err := p.jobs.UpdateStatus(ctx, job.ID, enginejob.StatusDelivering, ""); err != nil {
		return fmt.Errorf("mark job delivering: %w", err)
	}
	log.Info("delivery job marked delivering")

	var payload worksheetsexport.DeliveryPayload
	if err := json.Unmarshal(job.PayloadJSON, &payload); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("decode delivery job payload: %w", err))
	}
	log.Info(
		"delivery payload decoded",
		zap.String("prepare_job_id", payload.PrepareJobID),
		zap.String("temp_path", payload.TempPath),
	)

	data, err := p.storage.Read(ctx, payload.TempPath)
	if err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("read prepare temp file: %w", err))
	}
	log.Info("prepare temp file loaded", zap.Int("bytes", len(data)))

	finalPath, err := p.storage.Save(ctx, filepath.Join(job.ID+".json"), data)
	if err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("save delivery final file: %w", err))
	}
	log.Info("delivery final file saved", zap.String("result_path", finalPath))

	if err := p.jobs.UpdateResult(ctx, job.ID, finalPath); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("update delivery result path: %w", err))
	}

	if err := p.storage.Delete(ctx, payload.TempPath); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("delete prepare temp file: %w", err))
	}
	log.Info("prepare temp file deleted", zap.String("temp_path", payload.TempPath))

	if err := p.jobs.UpdateStatus(ctx, job.ID, enginejob.StatusDone, ""); err != nil {
		return fmt.Errorf("mark job done: %w", err)
	}
	log.Info("delivery job marked done", zap.String("result_path", finalPath))

	if payload.PrepareJobID != "" {
		if err := p.jobs.UpdateStatus(ctx, payload.PrepareJobID, enginejob.StatusDone, ""); err != nil {
			return fmt.Errorf("mark prepare job done after delivery: %w", err)
		}
		log.Info("prepare job marked done after delivery", zap.String("prepare_job_id", payload.PrepareJobID))
	}

	return nil
}

func (p *DeliveryProcessor) recordFailure(ctx context.Context, log *logger.Logger, job enginejob.Job, err error) error {
	log.Error("delivery job processing failed", zap.Error(err))
	if recordErr := recordProcessingFailure(ctx, p.retry, p.jobs, p.outbox, p.ids, DeliveryTopic, job, err); recordErr != nil {
		return recordErr
	}
	return nil
}
