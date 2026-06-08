package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type carrier map[string]string

func (c carrier) Get(key string) string {
	return c[key]
}

func (c carrier) Set(key string, value string) {
	c[key] = value
}

func (c carrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func InjectHeaders(ctx context.Context) map[string]string {
	headers := make(carrier)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	if len(headers) == 0 {
		return nil
	}
	return headers
}

func ExtractContext(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier(headers))
}

func TraceFields(ctx context.Context) (traceID string, spanID string) {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return "", ""
	}
	return spanContext.TraceID().String(), spanContext.SpanID().String()
}

func Propagator() propagation.TextMapPropagator {
	return otel.GetTextMapPropagator()
}
