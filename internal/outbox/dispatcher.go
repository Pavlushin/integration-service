package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"onec-integration/internal/logger"
	"onec-integration/internal/queue"
	"onec-integration/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

type Dispatcher struct {
	reader    Reader
	publisher queue.Publisher
	interval  time.Duration
	batchSize int
}

func NewDispatcher(reader Reader, publisher queue.Publisher, interval time.Duration, batchSize int) (*Dispatcher, error) {
	switch {
	case reader == nil:
		return nil, fmt.Errorf("outbox reader is required")
	case publisher == nil:
		return nil, fmt.Errorf("queue publisher is required")
	}

	if interval <= 0 {
		interval = time.Second
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	return &Dispatcher{
		reader:    reader,
		publisher: publisher,
		interval:  interval,
		batchSize: batchSize,
	}, nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	log := logger.FromContext(ctx).With(zap.String("component", "outbox_dispatcher"))
	log.Info("outbox dispatcher loop started")

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	defer log.Info("outbox dispatcher loop stopped")

	for {
		if err := d.dispatchBatch(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (d *Dispatcher) dispatchBatch(ctx context.Context) error {
	log := logger.FromContext(ctx)
	records, err := d.reader.ListPending(ctx, d.batchSize)
	if err != nil {
		return fmt.Errorf("list pending outbox records: %w", err)
	}
	if len(records) > 0 {
		log.Debug("outbox batch loaded", zap.Int("count", len(records)))
	}

	for _, record := range records {
		recordCtx := telemetry.ExtractContext(ctx, record.Headers)
		recordCtx, span := telemetry.Tracer().Start(recordCtx, "outbox.publish_record")
		span.SetAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", record.Topic),
			attribute.String("outbox.id", record.ID),
		)

		recordLog := log.With(
			zap.String("outbox_id", record.ID),
			zap.String("topic", record.Topic),
		)

		var message queue.Message
		if err := json.Unmarshal(record.PayloadJSON, &message); err != nil {
			_ = d.reader.MarkFailed(recordCtx, record.ID, fmt.Sprintf("decode outbox payload: %v", err))
			recordLog.Error("outbox payload decode failed", zap.Error(err))
			span.RecordError(err)
			span.SetStatus(codes.Error, "decode outbox payload")
			span.End()
			continue
		}
		recordLog = recordLog.With(zap.String("job_id", message.JobID))
		recordLog.Info("publishing outbox record to queue")
		traceHeaders := telemetry.InjectHeaders(recordCtx)

		if err := d.publisher.PublishJob(recordCtx, record.Topic, message, traceHeaders); err != nil {
			_ = d.reader.MarkFailed(recordCtx, record.ID, err.Error())
			recordLog.Error("queue publish failed", zap.Error(err))
			span.RecordError(err)
			span.SetStatus(codes.Error, "publish queue message")
			span.End()
			continue
		}

		if err := d.reader.MarkPublished(recordCtx, record.ID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "mark outbox published")
			span.End()
			return fmt.Errorf("mark outbox record published: %w", err)
		}
		recordLog.Info("outbox record published")
		span.End()
	}

	return nil
}
