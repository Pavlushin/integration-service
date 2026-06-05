package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"onec-integration/internal/client/product"
	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	"onec-integration/internal/outbox"
	"onec-integration/internal/storage"
	"onec-integration/internal/worksheetsexport"

	"go.uber.org/zap"
)

type PrepareProcessor struct {
	jobs    JobRepository
	outbox  OutboxWriter
	ids     IDGenerator
	storage storage.Storage
	client  *product.Client
}

func NewPrepareProcessor(
	jobs JobRepository,
	outboxWriter OutboxWriter,
	ids IDGenerator,
	storageProvider storage.Storage,
	client *product.Client,
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
	case client == nil:
		return nil, fmt.Errorf("product client is required")
	}

	return &PrepareProcessor{
		jobs:    jobs,
		outbox:  outboxWriter,
		ids:     ids,
		storage: storageProvider,
		client:  client,
	}, nil
}

func (p *PrepareProcessor) Process(ctx context.Context, jobID string) error {
	if p == nil {
		return fmt.Errorf("prepare processor is nil")
	}

	log := logger.FromContext(ctx).With(zap.String("job_id", jobID), zap.String("stage", "prepare"))
	log.Info("prepare processing started")
	startedAt := time.Now()
	defer func() {
		log.Info("prepare processing finished", zap.Duration("latency", time.Since(startedAt)))
	}()

	job, err := p.jobs.GetByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load prepare job: %w", err)
	}
	log = log.With(
		zap.String("correlation_id", job.CorrelationID),
		zap.String("job_type", job.Type),
	)

	if job.Kind != enginejob.KindPrepare {
		return fmt.Errorf("unexpected job kind: %s", job.Kind)
	}

	if job.Status == enginejob.StatusPrepared || job.Status == enginejob.StatusDone {
		return nil
	}

	if err := p.jobs.UpdateStatus(ctx, job.ID, enginejob.StatusValidated, ""); err != nil {
		return fmt.Errorf("mark job validated: %w", err)
	}
	log.Info("prepare job validated")

	var request worksheetsexport.Request
	if err := json.Unmarshal(job.PayloadJSON, &request); err != nil {
		return fmt.Errorf("decode prepare job payload: %w", err)
	}
	log.Info(
		"calling product api for worksheets export",
		zap.String("date_from", request.DateFrom),
		zap.String("date_to", request.DateTo),
	)

	apiStartedAt := time.Now()
	responseBody, err := p.client.ExportWorksheets(ctx, request)
	if err != nil {
		return fmt.Errorf("export worksheets: %w", err)
	}
	log.Info(
		"product api response received",
		zap.Duration("latency", time.Since(apiStartedAt)),
		zap.Int("bytes", len(responseBody)),
	)

	tempPath, err := p.storage.Save(ctx, filepath.Join("tmp", job.ID+".json"), responseBody)
	if err != nil {
		return fmt.Errorf("save prepare temp file: %w", err)
	}
	log.Info("prepare temp file saved", zap.String("result_path", tempPath))

	if err := p.jobs.UpdateResult(ctx, job.ID, tempPath); err != nil {
		return fmt.Errorf("update prepare result path: %w", err)
	}

	if err := p.jobs.UpdateStatus(ctx, job.ID, enginejob.StatusPrepared, ""); err != nil {
		return fmt.Errorf("mark job prepared: %w", err)
	}
	log.Info("prepare job marked prepared", zap.String("result_path", tempPath))

	deliveryPayload, err := json.Marshal(worksheetsexport.DeliveryPayload{
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

	if err := p.jobs.Create(ctx, deliveryJob); err != nil {
		return fmt.Errorf("create delivery job: %w", err)
	}
	log.Info(
		"delivery job created",
		zap.String("delivery_job_id", deliveryJob.ID),
		zap.String("delivery_status", string(deliveryJob.Status)),
	)

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
		CreatedAt:   time.Now().UTC(),
	}
	if err := p.outbox.Enqueue(ctx, record); err != nil {
		return fmt.Errorf("enqueue delivery outbox record: %w", err)
	}
	log.Info(
		"delivery outbox record enqueued",
		zap.String("outbox_id", record.ID),
		zap.String("topic", record.Topic),
	)

	return nil
}
