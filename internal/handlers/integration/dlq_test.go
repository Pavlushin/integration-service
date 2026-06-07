package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"onec-integration/internal/engine"
	enginejob "onec-integration/internal/engine/job"

	"github.com/go-chi/chi/v5"
)

type fakeDLQController struct {
	listLimit    int
	requeueID    string
	listErr      error
	requeueErr   error
	requeuedJob  enginejob.Job
	requeueInput engine.RequeueDLQInput
	auditJobID   string
}

func (c *fakeDLQController) ListDLQJobs(_ context.Context, limit int) ([]enginejob.Job, error) {
	c.listLimit = limit
	if c.listErr != nil {
		return nil, c.listErr
	}
	return []enginejob.Job{{
		ID:            "job-1",
		CorrelationID: "corr-1",
		Type:          "worksheets_export_test",
		Kind:          enginejob.KindPrepare,
		Direction:     enginejob.DirectionInbound,
		Status:        enginejob.StatusDLQ,
		Attempts:      3,
		LastError:     "product api unavailable",
		CreatedAt:     time.Date(2026, 6, 5, 15, 0, 0, 0, time.UTC),
	}}, nil
}

func (c *fakeDLQController) RequeueDLQJob(_ context.Context, input engine.RequeueDLQInput) (enginejob.Job, error) {
	c.requeueID = input.JobID
	c.requeueInput = input
	if c.requeueErr != nil {
		return enginejob.Job{}, c.requeueErr
	}
	return c.requeuedJob, nil
}

func (c *fakeDLQController) ListJobAudit(_ context.Context, jobID string, _ int) ([]engine.JobAuditRecord, error) {
	c.auditJobID = jobID
	return []engine.JobAuditRecord{{
		ID:     "audit-1",
		JobID:  jobID,
		Action: engine.JobAuditActionDLQRequeue,
		Actor:  "codex",
		Reason: "manual retry",
	}}, nil
}

func TestDLQListRoute(t *testing.T) {
	router := chi.NewRouter()
	controller := &fakeDLQController{}
	RegisterDLQRoutes(router, controller)

	req := httptest.NewRequest(http.MethodGet, "/integration/api/v1/dlq/jobs?limit=7", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if controller.listLimit != 7 {
		t.Fatalf("expected list limit 7, got %d", controller.listLimit)
	}

	var body struct {
		Jobs []struct {
			ID        string `json:"id"`
			Status    string `json:"status"`
			Attempts  int    `json:"attempts"`
			LastError string `json:"last_error"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Jobs) != 1 || body.Jobs[0].ID != "job-1" || body.Jobs[0].Status != "dlq" {
		t.Fatalf("unexpected jobs response: %#v", body.Jobs)
	}
	if body.Jobs[0].Attempts != 3 || body.Jobs[0].LastError != "product api unavailable" {
		t.Fatalf("unexpected dlq metadata: %#v", body.Jobs[0])
	}
}

func TestDLQRequeueRoute(t *testing.T) {
	router := chi.NewRouter()
	controller := &fakeDLQController{
		requeuedJob: enginejob.Job{
			ID:       "job-1",
			Status:   enginejob.StatusRetrying,
			Attempts: 0,
		},
	}
	RegisterDLQRoutes(router, controller)

	requestBody := bytes.NewBufferString(`{"reason":"manual retry after product api recovery"}`)
	req := httptest.NewRequest(http.MethodPost, "/integration/api/v1/dlq/jobs/job-1/requeue", requestBody)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actor", "codex")
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", resp.Code, resp.Body.String())
	}
	if controller.requeueID != "job-1" {
		t.Fatalf("expected job-1 requeue, got %q", controller.requeueID)
	}
	if controller.requeueInput.Actor != "codex" {
		t.Fatalf("expected actor codex, got %q", controller.requeueInput.Actor)
	}
	if controller.requeueInput.Reason != "manual retry after product api recovery" {
		t.Fatalf("expected requeue reason, got %q", controller.requeueInput.Reason)
	}

	var responseBody struct {
		JobID    string `json:"job_id"`
		Status   string `json:"status"`
		Requeued bool   `json:"requeued"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&responseBody); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if responseBody.JobID != "job-1" || responseBody.Status != "retrying" || !responseBody.Requeued {
		t.Fatalf("unexpected requeue response: %#v", responseBody)
	}
}

func TestDLQRequeueRouteReturnsBadRequestForServiceError(t *testing.T) {
	router := chi.NewRouter()
	controller := &fakeDLQController{requeueErr: errors.New("job is not in dlq status")}
	RegisterDLQRoutes(router, controller)

	req := httptest.NewRequest(http.MethodPost, "/integration/api/v1/dlq/jobs/job-1/requeue", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestDLQAuditRoute(t *testing.T) {
	router := chi.NewRouter()
	controller := &fakeDLQController{}
	RegisterDLQRoutes(router, controller)

	req := httptest.NewRequest(http.MethodGet, "/integration/api/v1/dlq/jobs/job-1/audit", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if controller.auditJobID != "job-1" {
		t.Fatalf("expected audit lookup for job-1, got %q", controller.auditJobID)
	}

	var body struct {
		Audit []struct {
			ID     string `json:"id"`
			JobID  string `json:"job_id"`
			Action string `json:"action"`
			Actor  string `json:"actor"`
			Reason string `json:"reason"`
		} `json:"audit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode audit response: %v", err)
	}
	if len(body.Audit) != 1 || body.Audit[0].ID != "audit-1" || body.Audit[0].Action != engine.JobAuditActionDLQRequeue {
		t.Fatalf("unexpected audit response: %#v", body.Audit)
	}
}
