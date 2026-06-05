package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/outbox"
)

type retryJobRepository struct {
	createdJob      enginejob.Job
	statusJobID     string
	status          enginejob.Status
	statusLastError string
	incrementJobID  string
	incrementError  string
}

func (r *retryJobRepository) Create(_ context.Context, job enginejob.Job) error {
	r.createdJob = job
	return nil
}

func (r *retryJobRepository) GetByID(_ context.Context, _ string) (enginejob.Job, error) {
	return enginejob.Job{}, errors.New("not implemented")
}

func (r *retryJobRepository) FindActiveByDedupeKey(_ context.Context, _ string) (enginejob.Job, bool, error) {
	return enginejob.Job{}, false, errors.New("not implemented")
}

func (r *retryJobRepository) UpdateStatus(_ context.Context, jobID string, status enginejob.Status, lastError string) error {
	r.statusJobID = jobID
	r.status = status
	r.statusLastError = lastError
	return nil
}

func (r *retryJobRepository) UpdateResult(_ context.Context, _ string, _ string) error {
	return errors.New("not implemented")
}

func (r *retryJobRepository) IncrementAttempts(_ context.Context, jobID string, lastError string) error {
	r.incrementJobID = jobID
	r.incrementError = lastError
	return nil
}

type retryOutboxWriter struct {
	records []outbox.Record
}

func (w *retryOutboxWriter) Enqueue(_ context.Context, record outbox.Record) error {
	w.records = append(w.records, record)
	return nil
}

type retryIDGenerator struct {
	next string
}

func (g retryIDGenerator) NewID() string {
	return g.next
}

func TestRecordProcessingFailureSchedulesRetryBeforeAttemptLimit(t *testing.T) {
	repo := &retryJobRepository{}
	outboxWriter := &retryOutboxWriter{}
	job := enginejob.Job{
		ID:       "job-1",
		Attempts: 0,
	}

	err := recordProcessingFailure(
		context.Background(),
		RetryPolicy{MaxAttempts: 4, Backoff: 2 * time.Second},
		repo,
		outboxWriter,
		retryIDGenerator{next: "retry-outbox-1"},
		PrepareTopic,
		job,
		errors.New("product api unavailable"),
	)

	if err != nil {
		t.Fatalf("expected retry scheduling to succeed, got %v", err)
	}
	if repo.incrementJobID != "job-1" {
		t.Fatalf("expected attempts increment for job-1, got %q", repo.incrementJobID)
	}
	if repo.incrementError != "product api unavailable" {
		t.Fatalf("expected last error to be recorded, got %q", repo.incrementError)
	}
	if repo.statusJobID != "job-1" || repo.status != enginejob.StatusRetrying {
		t.Fatalf("expected job-1 to be marked retrying, got %q/%q", repo.statusJobID, repo.status)
	}
	if repo.statusLastError != "product api unavailable" {
		t.Fatalf("expected retry status error to be recorded, got %q", repo.statusLastError)
	}
	if len(outboxWriter.records) != 1 {
		t.Fatalf("expected one retry outbox record, got %d", len(outboxWriter.records))
	}

	record := outboxWriter.records[0]
	if record.ID != "retry-outbox-1" {
		t.Fatalf("expected retry outbox id, got %q", record.ID)
	}
	if record.Topic != PrepareTopic {
		t.Fatalf("expected retry topic %q, got %q", PrepareTopic, record.Topic)
	}
	if record.CreatedAt.IsZero() || time.Since(record.CreatedAt) > time.Minute {
		t.Fatalf("expected recent created_at, got %v", record.CreatedAt)
	}
	if record.AvailableAt.Sub(record.CreatedAt) != 2*time.Second {
		t.Fatalf("expected retry available_at to be created_at + 2s, got %s", record.AvailableAt.Sub(record.CreatedAt))
	}

	var payload map[string]string
	if err := json.Unmarshal(record.PayloadJSON, &payload); err != nil {
		t.Fatalf("decode retry outbox payload: %v", err)
	}
	if payload["job_id"] != "job-1" {
		t.Fatalf("expected retry payload job_id job-1, got %q", payload["job_id"])
	}
}

func TestRecordProcessingFailureMovesJobToDLQAtAttemptLimit(t *testing.T) {
	repo := &retryJobRepository{}
	outboxWriter := &retryOutboxWriter{}
	policy := RetryPolicy{MaxAttempts: 5, Backoff: 2 * time.Second}
	job := enginejob.Job{
		ID:       "job-1",
		Attempts: policy.MaxAttempts - 1,
	}

	err := recordProcessingFailure(
		context.Background(),
		policy,
		repo,
		outboxWriter,
		retryIDGenerator{next: "retry-outbox-1"},
		PrepareTopic,
		job,
		errors.New("payload is invalid"),
	)

	if err != nil {
		t.Fatalf("expected dlq transition to succeed, got %v", err)
	}
	if repo.incrementJobID != "job-1" {
		t.Fatalf("expected attempts increment for job-1, got %q", repo.incrementJobID)
	}
	if repo.statusJobID != "job-1" || repo.status != enginejob.StatusDLQ {
		t.Fatalf("expected job-1 to be marked dlq, got %q/%q", repo.statusJobID, repo.status)
	}
	if repo.statusLastError != "payload is invalid" {
		t.Fatalf("expected dlq status error to be recorded, got %q", repo.statusLastError)
	}
	if len(outboxWriter.records) != 0 {
		t.Fatalf("expected no retry outbox records at attempt limit, got %d", len(outboxWriter.records))
	}
}

func TestRetryPolicyWithDefaults(t *testing.T) {
	policy := RetryPolicy{}

	normalized := policy.WithDefaults()

	if normalized.MaxAttempts != 3 {
		t.Fatalf("expected default max attempts 3, got %d", normalized.MaxAttempts)
	}
	if normalized.Backoff != time.Second {
		t.Fatalf("expected default backoff 1s, got %s", normalized.Backoff)
	}
}
