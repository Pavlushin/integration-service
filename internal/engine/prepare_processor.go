package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	"onec-integration/internal/outbox"
	"onec-integration/internal/storage"
	"onec-integration/internal/telemetry"
	"onec-integration/internal/worksheets/usersreports"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

type PrepareProcessor struct {
	jobs     JobRepository
	outbox   OutboxWriter
	ids      IDGenerator
	storage  storage.Storage
	exporter usersreports.Exporter
	retry    RetryPolicy
}

func NewPrepareProcessor(
	jobs JobRepository,
	outboxWriter OutboxWriter,
	ids IDGenerator,
	storageProvider storage.Storage,
	exporter usersreports.Exporter,
	retryPolicy RetryPolicy,
) (*PrepareProcessor, error) {
	switch {
	case jobs == nil:
		return nil, fmt.Errorf("job repository is required")
	case outboxWriter == nil:
		return nil, fmt.Errorf("outbox writer is required")
	case ids == nil:
		return nil, fmt.Errorf("id generator is required")
	case storageProvider == nil:
		return nil, fmt.Errorf("storage is required")
	case exporter == nil:
		return nil, fmt.Errorf("users reports exporter is required")
	}

	return &PrepareProcessor{
		jobs:     jobs,
		outbox:   outboxWriter,
		ids:      ids,
		storage:  storageProvider,
		exporter: exporter,
		retry:    retryPolicy.WithDefaults(),
	}, nil
}

func (p *PrepareProcessor) Process(ctx context.Context, jobID string) error {
	if p == nil {
		return fmt.Errorf("prepare processor is nil")
	}
	ctx, span := telemetry.Tracer().Start(ctx, "prepare.process")
	defer span.End()
	span.SetAttributes(attribute.String("job.id", jobID))

	log := logger.FromContext(ctx).With(zap.String("job_id", jobID), zap.String("stage", "prepare"))
	log.Info("prepare processing started")
	startedAt := time.Now()
	defer func() {
		log.Info("prepare processing finished", zap.Duration("latency", time.Since(startedAt)))
	}()

	job, err := p.jobs.GetByID(ctx, jobID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "load prepare job")
		return fmt.Errorf("load prepare job: %w", err)
	}
	log = log.With(
		zap.String("correlation_id", job.CorrelationID),
		zap.String("job_type", job.Type),
	)
	span.SetAttributes(
		attribute.String("job.correlation_id", job.CorrelationID),
		attribute.String("job.type", job.Type),
	)

	if job.Kind != enginejob.KindPrepare {
		return p.recordFailure(ctx, log, job, fmt.Errorf("unexpected job kind: %s", job.Kind))
	}

	if job.Status == enginejob.StatusPrepared || job.IsTerminal() {
		return nil
	}

	if err := p.jobs.UpdateStatus(ctx, job.ID, enginejob.StatusValidated, ""); err != nil {
		return fmt.Errorf("mark job validated: %w", err)
	}
	log.Info("prepare job validated")

	var request usersreports.Request
	if err := json.Unmarshal(job.PayloadJSON, &request); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("decode prepare job payload: %w", err))
	}
	log.Info(
		"building worksheets export from lk mariadb",
		zap.String("date_from", request.DateFrom),
		zap.String("date_to", request.DateTo),
	)

	exportStartedAt := time.Now()
	responseBody, err := p.exporter.Export(ctx, request)
	if err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("export worksheets: %w", err))
	}
	log.Info(
		"worksheets export built from lk mariadb",
		zap.Duration("latency", time.Since(exportStartedAt)),
		zap.Int("bytes", len(responseBody)),
	)

	tempPath, err := p.storage.Save(ctx, filepath.Join("tmp", job.ID+".json"), responseBody)
	if err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("save prepare temp file: %w", err))
	}
	log.Info("prepare temp file saved", zap.String("result_path", tempPath))

	if err := p.jobs.UpdateResult(ctx, job.ID, tempPath); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("update prepare result path: %w", err))
	}

	if err := p.jobs.UpdateStatus(ctx, job.ID, enginejob.StatusPrepared, ""); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("mark job prepared: %w", err))
	}
	log.Info("prepare job marked prepared", zap.String("result_path", tempPath))

	deliveryPayload, err := json.Marshal(usersreports.DeliveryPayload{
		PrepareJobID: job.ID,
		TempPath:     tempPath,
	})
	if err != nil {
		return fmt.Errorf("marshal delivery payload: %w", err)
	}

	deliveryJob := enginejob.Job{
		ID:             p.ids.NewID(),
		CorrelationID:  job.CorrelationID,
		ParentID:       job.ID,
		Type:           job.Type,
		Kind:           enginejob.KindDelivery,
		Direction:      enginejob.DirectionOutbound,
		Status:         enginejob.StatusPrepared,
		DedupeKey:      job.DedupeKey + ":delivery",
		IdempotencyKey: job.IdempotencyKey,
		PayloadJSON:    deliveryPayload,
		Attempts:       0,
		CreatedAt:      time.Now().UTC(),
	}

	messagePayload, err := json.Marshal(map[string]string{
		"job_id": deliveryJob.ID,
	})
	if err != nil {
		return fmt.Errorf("marshal delivery outbox payload: %w", err)
	}

	record := outbox.Record{
		ID:          p.ids.NewID(),
		Topic:       DeliveryTopic,
		PayloadJSON: messagePayload,
		Headers:     telemetry.InjectHeaders(ctx),
		CreatedAt:   time.Now().UTC(),
	}
	if err := p.jobs.CreateWithOutbox(ctx, deliveryJob, record); err != nil {
		return p.recordFailure(ctx, log, job, fmt.Errorf("create delivery job and outbox record: %w", err))
	}
	log.Info(
		"delivery job and outbox record created",
		zap.String("delivery_job_id", deliveryJob.ID),
		zap.String("delivery_status", string(deliveryJob.Status)),
		zap.String("outbox_id", record.ID),
		zap.String("topic", record.Topic),
	)

	return nil
}

func (p *PrepareProcessor) recordFailure(ctx context.Context, log *logger.Logger, job enginejob.Job, err error) error {
	log.Error("prepare job processing failed", zap.Error(err))
	if recordErr := recordProcessingFailure(ctx, p.retry, p.jobs, p.outbox, p.ids, PrepareTopic, job, err); recordErr != nil {
		return recordErr
	}
	return nil
}
