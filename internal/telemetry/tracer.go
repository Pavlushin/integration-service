package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "onec-integration"

func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}
