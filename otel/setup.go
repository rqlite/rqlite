package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
)

func noopCleanup() error { return nil }

func Setup(ctx context.Context, endpoint string) (cleanup func() error, err error) {
	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(endpoint),
	)
	if err != nil {
		return noopCleanup, fmt.Errorf("unable to configure OTLP Trace exporter: %w", err)
	}

	tracerProvider := trace.NewTracerProvider(trace.WithBatcher(exporter))
	otel.SetTracerProvider(tracerProvider)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))

	return func() error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("OTLP Trace exporter did not shut down cleanly: %w", err)
		}
		return nil
	}, nil
}
