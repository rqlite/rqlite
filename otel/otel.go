// Package otel provides OpenTelemetry tracing initialization for rqlite.
package otel

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const tracerName = "github.com/rqlite/rqlite"

// Config holds configuration for the OpenTelemetry tracing setup.
type Config struct {
	// Enabled controls whether tracing is active. When false,
	// a no-op TracerProvider is used and no spans are recorded.
	Enabled bool

	// NodeID identifies this rqlite node in trace data.
	NodeID string

	// Version is the software version of this rqlite node.
	Version string

	// Writer is the destination for the stdout exporter.
	// If nil, no stdout exporter is configured.
	Writer io.Writer
}

// Setup initializes the OpenTelemetry TracerProvider based on the given Config.
// It returns a shutdown function that should be called on application exit.
// If tracing is disabled, a no-op provider is installed and shutdown is a no-op.
func Setup(cfg Config) (shutdown func(context.Context) error, err error) {
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("rqlite"),
			semconv.ServiceVersion(cfg.Version),
			semconv.ServiceInstanceID(cfg.NodeID),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
	}

	if cfg.Writer != nil {
		exp, err := stdouttrace.New(stdouttrace.WithWriter(cfg.Writer))
		if err != nil {
			return nil, fmt.Errorf("create stdout exporter: %w", err)
		}
		// Use SimpleSpanProcessor for development so spans are exported immediately.
		opts = append(opts, sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer returns a Tracer scoped to the rqlite instrumentation library.
func Tracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(tracerName)
}
