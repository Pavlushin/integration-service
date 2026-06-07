package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"onec-integration/internal/engine"
	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	httpresponse "onec-integration/internal/transport/http/response"

	"github.com/go-chi/chi/v5"
)

type DLQController interface {
	ListDLQJobs(ctx context.Context, limit int) ([]enginejob.Job, error)
	RequeueDLQJob(ctx context.Context, input engine.RequeueDLQInput) (enginejob.Job, error)
	ListJobAudit(ctx context.Context, jobID string, limit int) ([]engine.JobAuditRecord, error)
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

type dlqAuditRecordResponse struct {
	ID           string          `json:"id"`
	JobID        string          `json:"job_id"`
	Action       string          `json:"action"`
	Actor        string          `json:"actor"`
	Reason       string          `json:"reason"`
	MetadataJSON json.RawMessage `json:"metadata_json"`
	CreatedAt    string          `json:"created_at"`
}

type dlqAuditResponse struct {
	Audit []dlqAuditRecordResponse `json:"audit"`
}

type dlqRequeueRequest struct {
	Reason string `json:"reason"`
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
		requeueRequest, err := decodeRequeueRequest(req)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusBadRequest, err, "invalid dlq requeue request")
			return
		}

		job, err := controller.RequeueDLQJob(req.Context(), engine.RequeueDLQInput{
			JobID:  jobID,
			Actor:  req.Header.Get("X-Actor"),
			Reason: requeueRequest.Reason,
		})
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

	router.Get("/integration/api/v1/dlq/jobs/{job_id}/audit", func(w http.ResponseWriter, req *http.Request) {
		log := logger.FromContext(req.Context())
		responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

		limit := 0
		if rawLimit := req.URL.Query().Get("limit"); rawLimit != "" {
			parsedLimit, err := strconv.Atoi(rawLimit)
			if err != nil {
				responseHandler.ErrorResponse(http.StatusBadRequest, err, "invalid audit limit")
				return
			}
			limit = parsedLimit
		}

		records, err := controller.ListJobAudit(req.Context(), chi.URLParam(req, "job_id"), limit)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, err, "list job audit")
			return
		}

		responseHandler.JSONResponse(dlqAuditResponse{Audit: mapAuditRecords(records)}, http.StatusOK)
	})
}

func decodeRequeueRequest(req *http.Request) (dlqRequeueRequest, error) {
	if req.Body == nil {
		return dlqRequeueRequest{}, nil
	}
	defer req.Body.Close()

	var body dlqRequeueRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		if err == io.EOF {
			return dlqRequeueRequest{}, nil
		}
		return dlqRequeueRequest{}, err
	}

	return body, nil
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

func mapAuditRecords(records []engine.JobAuditRecord) []dlqAuditRecordResponse {
	response := make([]dlqAuditRecordResponse, 0, len(records))
	for _, record := range records {
		response = append(response, dlqAuditRecordResponse{
			ID:           record.ID,
			JobID:        record.JobID,
			Action:       record.Action,
			Actor:        record.Actor,
			Reason:       record.Reason,
			MetadataJSON: record.MetadataJSON,
			CreatedAt:    formatTime(record.CreatedAt),
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
