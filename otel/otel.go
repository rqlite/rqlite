// Package otel provides OpenTelemetry tracing setup for rqlite.
package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Config holds OpenTelemetry configuration.
type Config struct {
	Enabled     bool
	Endpoint    string // OTLP gRPC endpoint (default: localhost:4317)
	ServiceName string // Service name for traces (default: rqlite)
	Insecure    bool   // Use insecure connection (default: true for local collector)
}

// TracerProvider wraps the OTel tracer provider for lifecycle management.
type TracerProvider struct {
	provider *sdktrace.TracerProvider
}

// Setup initializes OpenTelemetry tracing and returns a TracerProvider.
// Returns nil if tracing is not enabled.
func Setup(ctx context.Context, cfg Config) (*TracerProvider, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	// Create OTLP exporter options
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Create resource with service name
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, err
	}

	// Create TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Set global TracerProvider and propagator
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &TracerProvider{provider: tp}, nil
}

// Shutdown gracefully shuts down the tracer provider, flushing any pending spans.
func (tp *TracerProvider) Shutdown(ctx context.Context) error {
	if tp == nil || tp.provider == nil {
		return nil
	}
	return tp.provider.Shutdown(ctx)
}
