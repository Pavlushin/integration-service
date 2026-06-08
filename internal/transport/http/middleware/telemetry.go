package middleware

import (
	"net/http"

	"onec-integration/internal/telemetry"
	httpresponse "onec-integration/internal/transport/http/response"

	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

func Telemetry() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			spanName := r.Method + " " + r.URL.Path
			ctx, span := telemetry.Tracer().Start(r.Context(), spanName, trace.WithSpanKind(trace.SpanKindServer))
			defer span.End()

			span.SetAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.URLPath(r.URL.Path),
			)

			responseWriter := httpresponse.NewResponseWriter(w)
			next.ServeHTTP(responseWriter, r.WithContext(ctx))

			statusCode := responseWriter.StatusCode()
			span.SetAttributes(semconv.HTTPResponseStatusCode(statusCode))
			if statusCode >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(statusCode))
			}
		})
	}
}
