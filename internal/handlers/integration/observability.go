package integration

import (
	"context"
	"net/http"

	"onec-integration/internal/engine"
	"onec-integration/internal/logger"
	httpresponse "onec-integration/internal/transport/http/response"

	"github.com/go-chi/chi/v5"
)

type ObservabilityController interface {
	Snapshot(ctx context.Context) (engine.ObservabilitySnapshot, error)
}

type OutboxObservabilityResponse struct {
	Pending int `json:"pending"`
	Delayed int `json:"delayed"`
	Failed  int `json:"failed"`
}

type ObservabilityResponse struct {
	JobsByStatus map[string]int              `json:"jobs_by_status"`
	Outbox       OutboxObservabilityResponse `json:"outbox"`
}

func RegisterObservabilityRoutes(router chi.Router, controller ObservabilityController) {
	router.Get("/integration/api/v1/observability/metrics", func(w http.ResponseWriter, req *http.Request) {
		log := logger.FromContext(req.Context())
		responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

		snapshot, err := controller.Snapshot(req.Context())
		if err != nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, err, "get observability metrics")
			return
		}

		responseHandler.JSONResponse(mapObservabilitySnapshot(snapshot), http.StatusOK)
	})
}

func mapObservabilitySnapshot(snapshot engine.ObservabilitySnapshot) ObservabilityResponse {
	return ObservabilityResponse{
		JobsByStatus: snapshot.JobsByStatus,
		Outbox: OutboxObservabilityResponse{
			Pending: snapshot.Outbox.Pending,
			Delayed: snapshot.Outbox.Delayed,
			Failed:  snapshot.Outbox.Failed,
		},
	}
}
