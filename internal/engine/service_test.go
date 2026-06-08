package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/outbox"
	"onec-integration/internal/storage"
	"onec-integration/internal/workflow"
)

type startHeavyJobRepository struct {
	latestJob                   enginejob.Job
	latestFound                 bool
	createdJobs                 []enginejob.Job
	createdOutboxRecords        []outbox.Record
	createdInboxSource          string
	createdInboxIdempotencyKey  string
	createdInboxResponseJSON    json.RawMessage
	createWithOutboxErr         error
	createWithOutboxAndInboxErr error
	updatedStatus               enginejob.Status
}

func (r *startHeavyJobRepository) Create(_ context.Context, job enginejob.Job) error {
	r.createdJobs = append(r.createdJobs, job)
	return nil
}

func (r *startHeavyJobRepository) GetByID(_ context.Context, _ string) (enginejob.Job, error) {
	return enginejob.Job{}, errors.New("not implemented")
}

func (r *startHeavyJobRepository) FindLatestByDedupeKey(_ context.Context, _ string) (enginejob.Job, bool, error) {
	return r.latestJob, r.latestFound, nil
}

func (r *startHeavyJobRepository) CreateWithOutbox(_ context.Context, job enginejob.Job, record outbox.Record) error {
	if r.createWithOutboxErr != nil {
		return r.createWithOutboxErr
	}
	r.createdJobs = append(r.createdJobs, job)
	r.createdOutboxRecords = append(r.createdOutboxRecords, record)
	return nil
}

func (r *startHeavyJobRepository) CreateWithOutboxAndInbox(_ context.Context, job enginejob.Job, record outbox.Record, source string, idempotencyKey string, responseJSON json.RawMessage) error {
	if r.createWithOutboxAndInboxErr != nil {
		return r.createWithOutboxAndInboxErr
	}
	r.createdJobs = append(r.createdJobs, job)
	r.createdOutboxRecords = append(r.createdOutboxRecords, record)
	r.createdInboxSource = source
	r.createdInboxIdempotencyKey = idempotencyKey
	r.createdInboxResponseJSON = responseJSON
	return nil
}

func (r *startHeavyJobRepository) UpdateStatus(_ context.Context, _ string, status enginejob.Status, _ string) error {
	r.updatedStatus = status
	return nil
}

func (r *startHeavyJobRepository) UpdateResult(_ context.Context, _ string, _ string) error {
	return errors.New("not implemented")
}

func (r *startHeavyJobRepository) IncrementAttempts(_ context.Context, _ string, _ string) error {
	return errors.New("not implemented")
}

type startHeavyInboxRepository struct {
	responseJSON json.RawMessage
	found        bool
}

func (r startHeavyInboxRepository) SaveResponse(_ context.Context, _ string, _ string, _ json.RawMessage) error {
	return nil
}

func (r startHeavyInboxRepository) GetResponse(_ context.Context, _ string, _ string) (json.RawMessage, bool, error) {
	return r.responseJSON, r.found, nil
}

type sequenceIDGenerator struct {
	next []string
}

func (g *sequenceIDGenerator) NewID() string {
	if len(g.next) == 0 {
		return ""
	}
	id := g.next[0]
	g.next = g.next[1:]
	return id
}

func TestStartHeavyCreatesNewJobAfterDoneJobByDedupeKey(t *testing.T) {
	repo := &startHeavyJobRepository{
		latestFound: true,
		latestJob: enginejob.Job{
			ID:            "job-done",
			CorrelationID: "corr-done",
			Type:          "worksheets_export_test",
			Kind:          enginejob.KindPrepare,
			Direction:     enginejob.DirectionInbound,
			Status:        enginejob.StatusDone,
			DedupeKey:     "worksheets_export_test:2026-06-01:2026-06-02",
			CreatedAt:     time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC),
		},
	}
	service, err := NewService(
		repo,
		startHeavyInboxRepository{},
		storage.NoopStorage{},
		workflow.MustNewRegistry(),
		&sequenceIDGenerator{next: []string{"corr-new", "job-new", "outbox-new"}},
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	result, err := service.StartHeavy(context.Background(), StartHeavyInput{
		Source:         "integration_api",
		Type:           "worksheets_export_test",
		Direction:      enginejob.DirectionInbound,
		DedupeKey:      "worksheets_export_test:2026-06-01:2026-06-02",
		IdempotencyKey: "idem-1",
		PayloadJSON:    json.RawMessage(`{"date_from":"2026-06-01","date_to":"2026-06-02"}`),
	})

	if err != nil {
		t.Fatalf("expected new job after done job, got %v", err)
	}
	if result.Reused {
		t.Fatalf("expected done job not to be reused")
	}
	if result.Job.ID != "job-new" {
		t.Fatalf("expected new job to be returned, got %q", result.Job.ID)
	}
	if len(repo.createdJobs) != 1 {
		t.Fatalf("expected one new job to be created, got %d", len(repo.createdJobs))
	}
	if len(repo.createdOutboxRecords) != 1 {
		t.Fatalf("expected one outbox record for new job after done, got %d", len(repo.createdOutboxRecords))
	}
}

func TestStartHeavyDoesNotPersistJobWithoutOutboxRecord(t *testing.T) {
	repo := &startHeavyJobRepository{
		createWithOutboxAndInboxErr: errors.New("insert outbox record"),
	}
	service, err := NewService(
		repo,
		startHeavyInboxRepository{},
		storage.NoopStorage{},
		workflow.MustNewRegistry(),
		&sequenceIDGenerator{next: []string{"corr-new", "job-new", "outbox-new"}},
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	_, err = service.StartHeavy(context.Background(), StartHeavyInput{
		Source:         "integration_api",
		Type:           "worksheets_export_test",
		Direction:      enginejob.DirectionInbound,
		DedupeKey:      "worksheets_export_test:2026-06-03:2026-06-04",
		IdempotencyKey: "idem-2",
		PayloadJSON:    json.RawMessage(`{"date_from":"2026-06-03","date_to":"2026-06-04"}`),
	})

	if err == nil {
		t.Fatalf("expected transactional create to fail")
	}
	if len(repo.createdJobs) != 0 {
		t.Fatalf("expected no persisted job without outbox record, got %d", len(repo.createdJobs))
	}
	if len(repo.createdOutboxRecords) != 0 {
		t.Fatalf("expected no persisted outbox records on transaction failure, got %d", len(repo.createdOutboxRecords))
	}
}

func TestStartHeavyReusesInboxResponseByIdempotencyKey(t *testing.T) {
	repo := &startHeavyJobRepository{}
	service, err := NewService(
		repo,
		startHeavyInboxRepository{
			found:        true,
			responseJSON: json.RawMessage(`{"job_id":"job-existing","correlation_id":"corr-existing"}`),
		},
		storage.NoopStorage{},
		workflow.MustNewRegistry(),
		&sequenceIDGenerator{next: []string{"corr-new", "job-new", "outbox-new"}},
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	result, err := service.StartHeavy(context.Background(), StartHeavyInput{
		Source:         "integration_api",
		Type:           "worksheets_export_test",
		Direction:      enginejob.DirectionInbound,
		DedupeKey:      "worksheets_export_test:2026-06-05:2026-06-06",
		IdempotencyKey: "idem-repeat",
		PayloadJSON:    json.RawMessage(`{"date_from":"2026-06-05","date_to":"2026-06-06"}`),
	})

	if err != nil {
		t.Fatalf("expected inbox reuse to succeed, got %v", err)
	}
	if !result.Reused {
		t.Fatalf("expected inbox response to be reused")
	}
	if result.Job.ID != "job-existing" || result.Job.CorrelationID != "corr-existing" {
		t.Fatalf("expected existing job response, got %q/%q", result.Job.ID, result.Job.CorrelationID)
	}
	if len(repo.createdJobs) != 0 {
		t.Fatalf("expected no new jobs for reused idempotency key, got %d", len(repo.createdJobs))
	}
	if len(repo.createdOutboxRecords) != 0 {
		t.Fatalf("expected no outbox records for reused idempotency key, got %d", len(repo.createdOutboxRecords))
	}
}

func TestStartHeavyPersistsInboxResponseWithJobAndOutbox(t *testing.T) {
	repo := &startHeavyJobRepository{}
	service, err := NewService(
		repo,
		startHeavyInboxRepository{},
		storage.NoopStorage{},
		workflow.MustNewRegistry(),
		&sequenceIDGenerator{next: []string{"corr-new", "job-new", "outbox-new"}},
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	result, err := service.StartHeavy(context.Background(), StartHeavyInput{
		Source:         "integration_api",
		Type:           "worksheets_export_test",
		Direction:      enginejob.DirectionInbound,
		DedupeKey:      "worksheets_export_test:2026-06-07:2026-06-08",
		IdempotencyKey: "idem-new",
		PayloadJSON:    json.RawMessage(`{"date_from":"2026-06-07","date_to":"2026-06-08"}`),
	})

	if err != nil {
		t.Fatalf("expected new job creation to succeed, got %v", err)
	}
	if result.Reused {
		t.Fatalf("expected new job not to be reused")
	}
	if len(repo.createdJobs) != 1 {
		t.Fatalf("expected one job to be created, got %d", len(repo.createdJobs))
	}
	if len(repo.createdOutboxRecords) != 1 {
		t.Fatalf("expected one outbox record to be created, got %d", len(repo.createdOutboxRecords))
	}
	if repo.createdInboxSource != "integration_api" || repo.createdInboxIdempotencyKey != "idem-new" {
		t.Fatalf("expected inbox source/key to be saved, got %q/%q", repo.createdInboxSource, repo.createdInboxIdempotencyKey)
	}

	var inboxResponse struct {
		JobID         string `json:"job_id"`
		CorrelationID string `json:"correlation_id"`
	}
	if err := json.Unmarshal(repo.createdInboxResponseJSON, &inboxResponse); err != nil {
		t.Fatalf("decode saved inbox response: %v", err)
	}
	if inboxResponse.JobID != result.Job.ID || inboxResponse.CorrelationID != result.Job.CorrelationID {
		t.Fatalf("expected inbox response to point to created job, got %q/%q", inboxResponse.JobID, inboxResponse.CorrelationID)
	}
}
