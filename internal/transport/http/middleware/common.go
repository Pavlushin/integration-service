package middleware

import (
	"net/http"
	"time"

	"onec-integration/internal/logger"
	"onec-integration/internal/requestctx"
	"onec-integration/internal/telemetry"
	httpresponse "onec-integration/internal/transport/http/response"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	RequestIDHeader       = "X-Request-ID"
	requestIDLegacyHeader = "X-Request-Id"
	CorrelationIDHeader   = "X-Correlation-ID"
)

func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := r.Header.Get(RequestIDHeader)
			if requestID == "" {
				requestID = r.Header.Get(requestIDLegacyHeader)
			}
			if requestID == "" {
				requestID = uuid.NewString()
			}

			w.Header().Set(RequestIDHeader, requestID)
			ctx := requestctx.WithRequestID(r.Context(), requestID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func Logger(log *logger.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID, _ := requestctx.RequestID(r.Context())
			correlationID, _ := requestctx.CorrelationID(r.Context())

			requestLogger := log.With(
				zap.String("request_id", requestID),
				zap.String("correlation_id", correlationID),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
			)
			if traceID, spanID := telemetry.TraceFields(r.Context()); traceID != "" {
				requestLogger = requestLogger.With(
					zap.String("trace_id", traceID),
					zap.String("span_id", spanID),
				)
			}

			ctx := logger.IntoContext(r.Context(), requestLogger)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func CorrelationID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			correlationID := r.Header.Get(CorrelationIDHeader)
			if correlationID == "" {
				correlationID, _ = requestctx.RequestID(r.Context())
			}
			if correlationID == "" {
				correlationID = uuid.NewString()
			}

			w.Header().Set(CorrelationIDHeader, correlationID)
			ctx := requestctx.WithCorrelationID(r.Context(), correlationID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func Panic() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log := logger.FromContext(r.Context())
			responseHandler := httpresponse.NewHTTPResponseHandler(log, w)

			defer func() {
				if p := recover(); p != nil {
					responseHandler.PanicResponse(p, "unexpected panic during HTTP handling")
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

func Trace() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log := logger.FromContext(r.Context())
			responseWriter := httpresponse.NewResponseWriter(w)
			startedAt := time.Now()

			log.Debug("incoming http request", zap.Time("started_at", startedAt.UTC()))

			next.ServeHTTP(responseWriter, r)

			log.Debug(
				"done http request",
				zap.Int("status_code", responseWriter.StatusCode()),
				zap.Duration("latency", time.Since(startedAt)),
			)
		})
	}
}
