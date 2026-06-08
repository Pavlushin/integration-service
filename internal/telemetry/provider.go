package telemetry

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

type Provider struct {
	shutdown func(context.Context) error
}

func Init(ctx context.Context, config Config, serviceName string) (*Provider, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if !config.Enabled {
		return &Provider{shutdown: func(context.Context) error { return nil }}, nil
	}

	options, err := exporterOptions(config.ExporterEndpoint)
	if err != nil {
		return nil, err
	}

	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)

	return &Provider{shutdown: provider.Shutdown}, nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

func exporterOptions(rawEndpoint string) ([]otlptracehttp.Option, error) {
	if rawEndpoint == "" {
		rawEndpoint = "http://jaeger:4318"
	}

	parsed, err := url.Parse(rawEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse otel exporter endpoint: %w", err)
	}

	options := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(parsed.Host),
	}

	if parsed.Scheme == "http" {
		options = append(options, otlptracehttp.WithInsecure())
	}

	if path := strings.TrimSpace(parsed.Path); path != "" && path != "/" {
		options = append(options, otlptracehttp.WithURLPath(path))
	}

	return options, nil
}
