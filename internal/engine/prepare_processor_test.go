package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"onec-integration/internal/client/product"
	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/outbox"
	"onec-integration/internal/storage"
)

type prepareJobRepository struct {
	job                  enginejob.Job
	createdJobs          []enginejob.Job
	createdOutboxRecords []outbox.Record
	createWithOutboxErr  error
	statusUpdates        []enginejob.Status
	resultPath           string
	incrementJobID       string
	incrementError       string
}

func (r *prepareJobRepository) Create(_ context.Context, job enginejob.Job) error {
	r.createdJobs = append(r.createdJobs, job)
	return nil
}

func (r *prepareJobRepository) GetByID(_ context.Context, jobID string) (enginejob.Job, error) {
	if r.job.ID != jobID {
		return enginejob.Job{}, errors.New("job not found")
	}
	return r.job, nil
}

func (r *prepareJobRepository) FindLatestByDedupeKey(_ context.Context, _ string) (enginejob.Job, bool, error) {
	return enginejob.Job{}, false, errors.New("not implemented")
}

func (r *prepareJobRepository) CreateWithOutbox(_ context.Context, job enginejob.Job, record outbox.Record) error {
	if r.createWithOutboxErr != nil {
		return r.createWithOutboxErr
	}
	r.createdJobs = append(r.createdJobs, job)
	r.createdOutboxRecords = append(r.createdOutboxRecords, record)
	return nil
}

func (r *prepareJobRepository) UpdateStatus(_ context.Context, _ string, status enginejob.Status, _ string) error {
	r.statusUpdates = append(r.statusUpdates, status)
	return nil
}

func (r *prepareJobRepository) UpdateResult(_ context.Context, _ string, resultPath string) error {
	r.resultPath = resultPath
	return nil
}

func (r *prepareJobRepository) IncrementAttempts(_ context.Context, jobID string, lastError string) error {
	r.incrementJobID = jobID
	r.incrementError = lastError
	return nil
}

type prepareOutboxWriter struct {
	err     error
	records []outbox.Record
}

func (w *prepareOutboxWriter) Enqueue(_ context.Context, record outbox.Record) error {
	if w.err != nil {
		return w.err
	}
	w.records = append(w.records, record)
	return nil
}

func TestPrepareProcessorDoesNotPersistDeliveryJobWithoutOutboxRecord(t *testing.T) {
	productAPIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"source":"test-product-api"}`))
	}))
	defer productAPIServer.Close()

	repo := &prepareJobRepository{
		job: enginejob.Job{
			ID:             "prepare-1",
			CorrelationID:  "corr-1",
			Type:           "worksheets_export_test",
			Kind:           enginejob.KindPrepare,
			Direction:      enginejob.DirectionInbound,
			Status:         enginejob.StatusReceived,
			DedupeKey:      "worksheets_export_test:2026-06-03:2026-06-04",
			IdempotencyKey: "idem-1",
			PayloadJSON:    json.RawMessage(`{"date_from":"2026-06-03","date_to":"2026-06-04"}`),
			CreatedAt:      time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC),
		},
		createWithOutboxErr: errors.New("insert outbox record"),
	}
	outboxWriter := &prepareOutboxWriter{}
	processor, err := NewPrepareProcessor(
		repo,
		outboxWriter,
		&sequenceIDGenerator{next: []string{"delivery-1", "delivery-outbox-1", "retry-outbox-1"}},
		storage.NoopStorage{},
		product.NewClient(product.Config{
			BaseURL:     productAPIServer.URL,
			BearerToken: "test-token",
			Timeout:     time.Second,
		}),
		RetryPolicy{MaxAttempts: 3, Backoff: time.Second},
	)
	if err != nil {
		t.Fatalf("create prepare processor: %v", err)
	}

	if err := processor.Process(context.Background(), "prepare-1"); err != nil {
		t.Fatalf("expected prepare retry scheduling to succeed, got %v", err)
	}

	if len(repo.createdJobs) != 0 {
		t.Fatalf("expected no persisted delivery jobs without outbox record, got %d", len(repo.createdJobs))
	}
	if len(repo.createdOutboxRecords) != 0 {
		t.Fatalf("expected no persisted transactional outbox records, got %d", len(repo.createdOutboxRecords))
	}
	if repo.incrementJobID != "prepare-1" {
		t.Fatalf("expected prepare job attempts to be incremented, got %q", repo.incrementJobID)
	}
	if repo.incrementError == "" {
		t.Fatalf("expected prepare retry error to be recorded")
	}
	if len(outboxWriter.records) != 1 {
		t.Fatalf("expected one retry outbox record for prepare job, got %d", len(outboxWriter.records))
	}
}
