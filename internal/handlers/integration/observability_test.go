package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"onec-integration/internal/engine"

	"github.com/go-chi/chi/v5"
)

type fakeObservabilityController struct {
	called bool
}

func (c *fakeObservabilityController) Snapshot(context.Context) (engine.ObservabilitySnapshot, error) {
	c.called = true
	return engine.ObservabilitySnapshot{
		JobsByStatus: map[string]int{
			"dlq":      3,
			"retrying": 2,
		},
		Outbox: engine.OutboxObservability{
			Pending: 5,
			Delayed: 6,
			Failed:  7,
		},
	}, nil
}

func TestObservabilityRouteReturnsMetricsSnapshot(t *testing.T) {
	router := chi.NewRouter()
	controller := &fakeObservabilityController{}
	RegisterObservabilityRoutes(router, controller)

	req := httptest.NewRequest(http.MethodGet, "/integration/api/v1/observability/metrics", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if !controller.called {
		t.Fatal("expected observability controller to be called")
	}

	var body ObservabilityResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.JobsByStatus["dlq"] != 3 || body.JobsByStatus["retrying"] != 2 {
		t.Fatalf("unexpected job metrics: %#v", body.JobsByStatus)
	}
	if body.Outbox.Pending != 5 || body.Outbox.Delayed != 6 || body.Outbox.Failed != 7 {
		t.Fatalf("unexpected outbox metrics: %#v", body.Outbox)
	}
}
