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

type dlqJobRepository struct {
	job          enginejob.Job
	listedStatus enginejob.Status
	listedLimit  int
	resetJobID   string
}

func (r *dlqJobRepository) GetByID(_ context.Context, id string) (enginejob.Job, error) {
	if r.job.ID != id {
		return enginejob.Job{}, errors.New("job not found")
	}
	return r.job, nil
}

func (r *dlqJobRepository) ListByStatus(_ context.Context, status enginejob.Status, limit int) ([]enginejob.Job, error) {
	r.listedStatus = status
	r.listedLimit = limit
	return []enginejob.Job{r.job}, nil
}

func (r *dlqJobRepository) ResetForRetry(_ context.Context, jobID string) error {
	r.resetJobID = jobID
	return nil
}

type dlqOutboxWriter struct {
	records []outbox.Record
}

func (w *dlqOutboxWriter) Enqueue(_ context.Context, record outbox.Record) error {
	w.records = append(w.records, record)
	return nil
}

type dlqIDGenerator struct {
	next string
}

func (g dlqIDGenerator) NewID() string {
	return g.next
}

type dlqAuditWriter struct {
	records   []JobAuditRecord
	listJobID string
	listLimit int
}

func (w *dlqAuditWriter) RecordJobAudit(_ context.Context, record JobAuditRecord) error {
	w.records = append(w.records, record)
	return nil
}

func (w *dlqAuditWriter) ListJobAudit(_ context.Context, jobID string, limit int) ([]JobAuditRecord, error) {
	w.listJobID = jobID
	w.listLimit = limit
	return w.records, nil
}

func newTestDLQService(t *testing.T, jobs *dlqJobRepository, outboxWriter *dlqOutboxWriter, auditWriter *dlqAuditWriter) *DLQService {
	t.Helper()
	service, err := NewDLQService(jobs, outboxWriter, auditWriter, dlqIDGenerator{next: "outbox-1"})
	if err != nil {
		t.Fatalf("create dlq service: %v", err)
	}
	return service
}

func TestDLQServiceListsDLQJobs(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{ID: "job-1", Status: enginejob.StatusDLQ}}
	service := newTestDLQService(t, repo, &dlqOutboxWriter{}, &dlqAuditWriter{})

	jobs, err := service.ListDLQJobs(context.Background(), 25)

	if err != nil {
		t.Fatalf("list dlq jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "job-1" {
		t.Fatalf("expected job-1 in dlq list, got %#v", jobs)
	}
	if repo.listedStatus != enginejob.StatusDLQ {
		t.Fatalf("expected list by dlq status, got %q", repo.listedStatus)
	}
	if repo.listedLimit != 25 {
		t.Fatalf("expected limit 25, got %d", repo.listedLimit)
	}
}

func TestDLQServiceRequeuesDLQJob(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{
		ID:        "job-1",
		Kind:      enginejob.KindPrepare,
		Status:    enginejob.StatusDLQ,
		Attempts:  3,
		LastError: "product api unavailable",
	}}
	outboxWriter := &dlqOutboxWriter{}
	auditWriter := &dlqAuditWriter{}
	service := newTestDLQService(t, repo, outboxWriter, auditWriter)

	requeued, err := service.RequeueDLQJob(context.Background(), RequeueDLQInput{
		JobID:  "job-1",
		Actor:  "codex",
		Reason: "manual retry after product api recovery",
	})

	if err != nil {
		t.Fatalf("requeue dlq job: %v", err)
	}
	if repo.resetJobID != "job-1" {
		t.Fatalf("expected job-1 reset for retry, got %q", repo.resetJobID)
	}
	if requeued.Status != enginejob.StatusRetrying {
		t.Fatalf("expected requeued status retrying, got %q", requeued.Status)
	}
	if requeued.Attempts != 0 {
		t.Fatalf("expected attempts reset to 0, got %d", requeued.Attempts)
	}
	if requeued.LastError != "" {
		t.Fatalf("expected last_error reset, got %q", requeued.LastError)
	}
	if len(outboxWriter.records) != 1 {
		t.Fatalf("expected one outbox record, got %d", len(outboxWriter.records))
	}

	record := outboxWriter.records[0]
	if record.Topic != PrepareTopic {
		t.Fatalf("expected prepare topic, got %q", record.Topic)
	}
	if record.ID != "outbox-1" {
		t.Fatalf("expected outbox id, got %q", record.ID)
	}
	if record.CreatedAt.IsZero() || record.AvailableAt.IsZero() || record.AvailableAt.Sub(record.CreatedAt) != 0 {
		t.Fatalf("expected immediate outbox record, got created_at=%v available_at=%v", record.CreatedAt, record.AvailableAt)
	}

	var payload map[string]string
	if err := json.Unmarshal(record.PayloadJSON, &payload); err != nil {
		t.Fatalf("decode requeue payload: %v", err)
	}
	if payload["job_id"] != "job-1" {
		t.Fatalf("expected job_id job-1, got %q", payload["job_id"])
	}
	if len(auditWriter.records) != 1 {
		t.Fatalf("expected one audit record, got %d", len(auditWriter.records))
	}

	audit := auditWriter.records[0]
	if audit.JobID != "job-1" {
		t.Fatalf("expected audit job_id job-1, got %q", audit.JobID)
	}
	if audit.Action != JobAuditActionDLQRequeue {
		t.Fatalf("expected audit action %q, got %q", JobAuditActionDLQRequeue, audit.Action)
	}
	if audit.Actor != "codex" {
		t.Fatalf("expected audit actor codex, got %q", audit.Actor)
	}
	if audit.Reason != "manual retry after product api recovery" {
		t.Fatalf("expected audit reason, got %q", audit.Reason)
	}
	if audit.CreatedAt.IsZero() {
		t.Fatal("expected audit created_at to be set")
	}
	var metadata map[string]any
	if err := json.Unmarshal(audit.MetadataJSON, &metadata); err != nil {
		t.Fatalf("decode audit metadata: %v", err)
	}
	if metadata["status_before"] != "dlq" || metadata["status_after"] != "retrying" {
		t.Fatalf("unexpected audit metadata: %#v", metadata)
	}
}

func TestDLQServiceRejectsNonDLQJob(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{
		ID:     "job-1",
		Kind:   enginejob.KindPrepare,
		Status: enginejob.StatusRetrying,
	}}
	outboxWriter := &dlqOutboxWriter{}
	service := newTestDLQService(t, repo, outboxWriter, &dlqAuditWriter{})

	_, err := service.RequeueDLQJob(context.Background(), RequeueDLQInput{JobID: "job-1"})

	if err == nil {
		t.Fatal("expected non-dlq job requeue to fail")
	}
	if repo.resetJobID != "" {
		t.Fatalf("expected no reset for non-dlq job, got %q", repo.resetJobID)
	}
	if len(outboxWriter.records) != 0 {
		t.Fatalf("expected no outbox record for non-dlq job, got %d", len(outboxWriter.records))
	}
}

func TestDLQServiceRejectsUnknownJobKind(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{
		ID:     "job-1",
		Kind:   enginejob.Kind("unknown"),
		Status: enginejob.StatusDLQ,
	}}
	service := newTestDLQService(t, repo, &dlqOutboxWriter{}, &dlqAuditWriter{})

	_, err := service.RequeueDLQJob(context.Background(), RequeueDLQInput{JobID: "job-1"})

	if err == nil {
		t.Fatal("expected unknown job kind requeue to fail")
	}
}

func TestDLQServiceDefaultListLimit(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{ID: "job-1", Status: enginejob.StatusDLQ}}
	service := newTestDLQService(t, repo, &dlqOutboxWriter{}, &dlqAuditWriter{})

	_, err := service.ListDLQJobs(context.Background(), 0)

	if err != nil {
		t.Fatalf("list dlq jobs: %v", err)
	}
	if repo.listedLimit != defaultDLQLimit {
		t.Fatalf("expected default limit %d, got %d", defaultDLQLimit, repo.listedLimit)
	}
}

func TestDLQServiceCapsListLimit(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{ID: "job-1", Status: enginejob.StatusDLQ}}
	service := newTestDLQService(t, repo, &dlqOutboxWriter{}, &dlqAuditWriter{})

	_, err := service.ListDLQJobs(context.Background(), maxDLQLimit+1)

	if err != nil {
		t.Fatalf("list dlq jobs: %v", err)
	}
	if repo.listedLimit != maxDLQLimit {
		t.Fatalf("expected capped limit %d, got %d", maxDLQLimit, repo.listedLimit)
	}
}

func TestDLQServiceRequeueSetsRecentTimestamp(t *testing.T) {
	repo := &dlqJobRepository{job: enginejob.Job{
		ID:     "job-1",
		Kind:   enginejob.KindDelivery,
		Status: enginejob.StatusDLQ,
	}}
	outboxWriter := &dlqOutboxWriter{}
	service := newTestDLQService(t, repo, outboxWriter, &dlqAuditWriter{})

	_, err := service.RequeueDLQJob(context.Background(), RequeueDLQInput{JobID: "job-1"})

	if err != nil {
		t.Fatalf("requeue dlq job: %v", err)
	}
	record := outboxWriter.records[0]
	if record.Topic != DeliveryTopic {
		t.Fatalf("expected delivery topic, got %q", record.Topic)
	}
	if time.Since(record.CreatedAt) > time.Minute {
		t.Fatalf("expected recent created_at, got %v", record.CreatedAt)
	}
}

func TestDLQServiceListsJobAudit(t *testing.T) {
	auditWriter := &dlqAuditWriter{
		records: []JobAuditRecord{{
			ID:     "audit-1",
			JobID:  "job-1",
			Action: JobAuditActionDLQRequeue,
			Actor:  "codex",
			Reason: "manual retry",
		}},
	}
	service := newTestDLQService(
		t,
		&dlqJobRepository{job: enginejob.Job{ID: "job-1", Status: enginejob.StatusDLQ}},
		&dlqOutboxWriter{},
		auditWriter,
	)

	records, err := service.ListJobAudit(context.Background(), "job-1", 10)

	if err != nil {
		t.Fatalf("list job audit: %v", err)
	}
	if auditWriter.listJobID != "job-1" {
		t.Fatalf("expected audit lookup for job-1, got %q", auditWriter.listJobID)
	}
	if auditWriter.listLimit != 10 {
		t.Fatalf("expected audit limit 10, got %d", auditWriter.listLimit)
	}
	if len(records) != 1 || records[0].ID != "audit-1" {
		t.Fatalf("unexpected audit records: %#v", records)
	}
}
