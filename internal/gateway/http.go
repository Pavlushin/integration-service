package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"onec-integration/internal/engine"
	enginejob "onec-integration/internal/engine/job"
	"onec-integration/internal/logger"
	"onec-integration/internal/requestctx"
	httprequest "onec-integration/internal/transport/http/request"
	httpresponse "onec-integration/internal/transport/http/response"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

const IdempotencyKeyHeader = "X-Idempotency-Key"

type LowHandler[Req any, Resp any] func(ctx context.Context, req Req) (Resp, error)

type HeavyStarter interface {
	StartHeavy(ctx context.Context, input engine.StartHeavyInput) (engine.StartHeavyResult, error)
}

type Router struct {
	router  chi.Router
	starter HeavyStarter
}

func NewRouter(router chi.Router, starter HeavyStarter) *Router {
	return &Router{
		router:  router,
		starter: starter,
	}
}

type LowOptions struct {
	Method     string
	Path       string
	StatusCode int
}

type HighOptions[Req any] struct {
	Method      string
	Path        string
	Source      string
	Type        string
	Direction   enginejob.Direction
	BuildDedupe func(req Req) (string, error)
}

type highResponse struct {
	JobID         string `json:"job_id"`
	CorrelationID string `json:"correlation_id"`
	Reused        bool   `json:"reused"`
}

func RegisterLow[Req any, Resp any](r *Router, opts LowOptions, handler LowHandler[Req, Resp]) {
	statusCode := opts.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	r.route(opts.Method, opts.Path, func(w http.ResponseWriter, req *http.Request) {
		log := logger.FromContext(req.Context())
		responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

		var requestBody Req
		if err := httprequest.DecodeAndValidate(req, &requestBody); err != nil {
			responseHandler.ErrorResponse(http.StatusBadRequest, err, "invalid request payload")
			return
		}

		responseBody, err := handler(req.Context(), requestBody)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, err, "low request failed")
			return
		}

		responseHandler.JSONResponse(responseBody, statusCode)
	})
}

func RegisterHigh[Req any](r *Router, opts HighOptions[Req]) {
	r.route(opts.Method, opts.Path, func(w http.ResponseWriter, req *http.Request) {
		log := logger.FromContext(req.Context())
		responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

		if r.starter == nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, fmt.Errorf("heavy starter is not configured"), "heavy request is unavailable")
			return
		}

		var requestBody Req
		if err := httprequest.DecodeAndValidate(req, &requestBody); err != nil {
			responseHandler.ErrorResponse(http.StatusBadRequest, err, "invalid request payload")
			return
		}

		payloadJSON, err := json.Marshal(requestBody)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, err, "encode heavy request payload")
			return
		}

		dedupeKey, err := opts.BuildDedupe(requestBody)
		if err != nil {
			responseHandler.ErrorResponse(http.StatusBadRequest, err, "build dedupe key")
			return
		}

		correlationID, _ := requestctx.CorrelationID(req.Context())
		idempotencyKey := req.Header.Get(IdempotencyKeyHeader)
		log.Info(
			"heavy request accepted",
			zap.String("source", opts.Source),
			zap.String("type", opts.Type),
			zap.String("direction", string(opts.Direction)),
			zap.String("idempotency_key", idempotencyKey),
			zap.String("dedupe_key", dedupeKey),
		)

		result, err := r.starter.StartHeavy(req.Context(), engine.StartHeavyInput{
			Source:         opts.Source,
			CorrelationID:  correlationID,
			Type:           opts.Type,
			Direction:      opts.Direction,
			DedupeKey:      dedupeKey,
			IdempotencyKey: idempotencyKey,
			PayloadJSON:    payloadJSON,
		})
		if err != nil {
			responseHandler.ErrorResponse(http.StatusInternalServerError, err, "start heavy request")
			return
		}

		log.Info(
			"heavy request queued",
			zap.String("job_id", result.Job.ID),
			zap.Bool("reused", result.Reused),
		)

		responseHandler.JSONResponse(highResponse{
			JobID:         result.Job.ID,
			CorrelationID: result.Job.CorrelationID,
			Reused:        result.Reused,
		}, http.StatusAccepted)
	})
}

func (r *Router) route(method string, path string, handler http.HandlerFunc) {
	if r == nil || r.router == nil {
		panic("gateway router is nil")
	}

	switch method {
	case http.MethodGet:
		r.router.Get(path, handler)
	case http.MethodPost:
		r.router.Post(path, handler)
	case http.MethodPut:
		r.router.Put(path, handler)
	case http.MethodPatch:
		r.router.Patch(path, handler)
	case http.MethodDelete:
		r.router.Delete(path, handler)
	default:
		panic(fmt.Sprintf("unsupported method: %s", method))
	}
}
