package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/outbox"
)

const (
	defaultMaxProcessingAttempts = 3
	defaultRetryBackoff          = time.Second
)

type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

func (p RetryPolicy) WithDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = defaultMaxProcessingAttempts
	}
	if p.Backoff <= 0 {
		p.Backoff = defaultRetryBackoff
	}
	return p
}

func recordProcessingFailure(
	ctx context.Context,
	policy RetryPolicy,
	jobs JobRepository,
	outboxWriter OutboxWriter,
	ids IDGenerator,
	retryTopic string,
	job enginejob.Job,
	cause error,
) error {
	if jobs == nil {
		return fmt.Errorf("job repository is required")
	}
	if outboxWriter == nil {
		return fmt.Errorf("outbox writer is required")
	}
	if ids == nil {
		return fmt.Errorf("id generator is required")
	}
	if retryTopic == "" {
		return fmt.Errorf("retry topic is required")
	}
	if cause == nil {
		cause = errors.New("processing failed")
	}
	policy = policy.WithDefaults()

	lastError := cause.Error()
	if err := jobs.IncrementAttempts(ctx, job.ID, lastError); err != nil {
		return fmt.Errorf("increment job attempts after processing failure: %w", err)
	}

	nextAttempt := job.Attempts + 1
	if nextAttempt >= policy.MaxAttempts {
		if err := jobs.UpdateStatus(ctx, job.ID, enginejob.StatusDLQ, lastError); err != nil {
			return fmt.Errorf("mark job dlq after processing failure: %w", err)
		}
		return nil
	}

	if err := jobs.UpdateStatus(ctx, job.ID, enginejob.StatusRetrying, lastError); err != nil {
		return fmt.Errorf("mark job retrying after processing failure: %w", err)
	}

	messagePayload, err := json.Marshal(map[string]string{
		"job_id": job.ID,
	})
	if err != nil {
		return fmt.Errorf("marshal retry outbox payload: %w", err)
	}

	now := time.Now().UTC()
	record := outbox.Record{
		ID:          ids.NewID(),
		Topic:       retryTopic,
		PayloadJSON: messagePayload,
		CreatedAt:   now,
		AvailableAt: now.Add(policy.Backoff),
	}
	if err := outboxWriter.Enqueue(ctx, record); err != nil {
		return fmt.Errorf("enqueue retry outbox record: %w", err)
	}

	return nil
}
