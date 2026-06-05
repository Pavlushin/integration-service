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
	latestJob            enginejob.Job
	latestFound          bool
	createdJobs          []enginejob.Job
	createdOutboxRecords []outbox.Record
	createWithOutboxErr  error
	updatedStatus        enginejob.Status
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

type startHeavyInboxRepository struct{}

func (startHeavyInboxRepository) SaveResponse(_ context.Context, _ string, _ string, _ json.RawMessage) error {
	return errors.New("not implemented")
}

func (startHeavyInboxRepository) GetResponse(_ context.Context, _ string, _ string) (json.RawMessage, bool, error) {
	return nil, false, errors.New("not implemented")
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

func TestStartHeavyReusesDoneJobByDedupeKey(t *testing.T) {
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
		Type:           "worksheets_export_test",
		Direction:      enginejob.DirectionInbound,
		DedupeKey:      "worksheets_export_test:2026-06-01:2026-06-02",
		IdempotencyKey: "idem-1",
		PayloadJSON:    json.RawMessage(`{"date_from":"2026-06-01","date_to":"2026-06-02"}`),
	})

	if err != nil {
		t.Fatalf("expected done job reuse to succeed, got %v", err)
	}
	if !result.Reused {
		t.Fatalf("expected done job to be reused")
	}
	if result.Job.ID != "job-done" {
		t.Fatalf("expected job-done to be returned, got %q", result.Job.ID)
	}
	if len(repo.createdJobs) != 0 {
		t.Fatalf("expected no new jobs to be created, got %d", len(repo.createdJobs))
	}
	if len(repo.createdOutboxRecords) != 0 {
		t.Fatalf("expected no outbox records for reused done job, got %d", len(repo.createdOutboxRecords))
	}
}

func TestStartHeavyDoesNotPersistJobWithoutOutboxRecord(t *testing.T) {
	repo := &startHeavyJobRepository{
		createWithOutboxErr: errors.New("insert outbox record"),
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
