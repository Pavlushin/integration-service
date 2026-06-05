package integration

import (
	"context"
	"net/http"
	"strconv"
	"time"

	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	httpresponse "onec-integration/internal/transport/http/response"

	"github.com/go-chi/chi/v5"
)

type DLQController interface {
	ListDLQJobs(ctx context.Context, limit int) ([]enginejob.Job, error)
	RequeueDLQJob(ctx context.Context, jobID string) (enginejob.Job, error)
}

type dlqJobResponse struct {
	ID            string `json:"id"`
	CorrelationID string `json:"correlation_id"`
	ParentID      string `json:"parent_id,omitempty"`
	Type          string `json:"type"`
	Kind          string `json:"kind"`
	Direction     string `json:"direction"`
	Status        string `json:"status"`
	DedupeKey     string `json:"dedupe_key"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"last_error"`
	ResultPath    string `json:"result_path,omitempty"`
	CreatedAt     string `json:"created_at"`
}

type dlqListResponse struct {
	Jobs []dlqJobResponse `json:"jobs"`
}

type dlqRequeueResponse struct {
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	Requeued bool   `json:"requeued"`
}

func RegisterDLQRoutes(router chi.Router, controller DLQController) {
	router.Get("/integration/api/v1/dlq/jobs", func(w http.ResponseWriter, req *http.Request) {
		log := logger.FromContext(req.Context())
		responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

		limit := 0
		if rawLimit := req.URL.Query().Get("limit"); rawLimit != "" {
			parsedLimit, err := strconv.Atoi(rawLimit)
			if err != nil {
				responseHandler.ErrorResponse(http.StatusBadRequest, err, "invalid dlq limit")
				return
			}
			limit = parsedLimit
		}

		jobs, err := controller.ListDLQJobs(req.Context(), limit)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, err, "list dlq jobs")
			return
		}

		responseHandler.JSONResponse(dlqListResponse{Jobs: mapDLQJobs(jobs)}, http.StatusOK)
	})

	router.Post("/integration/api/v1/dlq/jobs/{job_id}/requeue", func(w http.ResponseWriter, req *http.Request) {
		log := logger.FromContext(req.Context())
		responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

		jobID := chi.URLParam(req, "job_id")
		job, err := controller.RequeueDLQJob(req.Context(), jobID)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusBadRequest, err, "requeue dlq job")
			return
		}

		responseHandler.JSONResponse(dlqRequeueResponse{
			JobID:    job.ID,
			Status:   string(job.Status),
			Requeued: true,
		}, http.StatusAccepted)
	})
}

func mapDLQJobs(jobs []enginejob.Job) []dlqJobResponse {
	response := make([]dlqJobResponse, 0, len(jobs))
	for _, job := range jobs {
		response = append(response, dlqJobResponse{
			ID:            job.ID,
			CorrelationID: job.CorrelationID,
			ParentID:      job.ParentID,
			Type:          job.Type,
			Kind:          string(job.Kind),
			Direction:     string(job.Direction),
			Status:        string(job.Status),
			DedupeKey:     job.DedupeKey,
			Attempts:      job.Attempts,
			LastError:     job.LastError,
			ResultPath:    job.ResultPath,
			CreatedAt:     formatTime(job.CreatedAt),
		})
	}
	return response
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
