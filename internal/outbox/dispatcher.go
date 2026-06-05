package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"onec-integration/internal/logger"
	"onec-integration/internal/queue"

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
		recordLog := log.With(
			zap.String("outbox_id", record.ID),
			zap.String("topic", record.Topic),
		)

		var message queue.Message
		if err := json.Unmarshal(record.PayloadJSON, &message); err != nil {
			_ = d.reader.MarkFailed(ctx, record.ID, fmt.Sprintf("decode outbox payload: %v", err))
			recordLog.Error("outbox payload decode failed", zap.Error(err))
			continue
		}
		recordLog = recordLog.With(zap.String("job_id", message.JobID))
		recordLog.Info("publishing outbox record to queue")

		if err := d.publisher.PublishJob(ctx, record.Topic, message); err != nil {
			_ = d.reader.MarkFailed(ctx, record.ID, err.Error())
			recordLog.Error("queue publish failed", zap.Error(err))
			continue
		}

		if err := d.reader.MarkPublished(ctx, record.ID); err != nil {
			return fmt.Errorf("mark outbox record published: %w", err)
		}
		recordLog.Info("outbox record published")
	}

	return nil
}
