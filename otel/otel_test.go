package otel

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

func Test_Setup_Disabled(t *testing.T) {
	shutdown, err := Setup(Config{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Should install a no-op provider.
	tp := otel.GetTracerProvider()
	if _, ok := tp.(noop.TracerProvider); !ok {
		t.Fatalf("expected noop.TracerProvider, got %T", tp)
	}

	// Shutdown should be a no-op.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %s", err)
	}
}

func Test_Setup_Enabled_StdoutExporter(t *testing.T) {
	var buf bytes.Buffer
	shutdown, err := Setup(Config{
		Enabled: true,
		NodeID:  "test-node-1",
		Version: "1.0.0-test",
		Writer:  &buf,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer shutdown(context.Background())

	// Create a span and end it to trigger export.
	tracer := Tracer()
	_, span := tracer.Start(context.Background(), "test-span")
	span.End()

	// Flush by shutting down.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %s", err)
	}

	// Check that the span was written to the buffer.
	output := buf.String()
	if !strings.Contains(output, "test-span") {
		t.Fatalf("expected stdout output to contain 'test-span', got: %s", output)
	}
	if !strings.Contains(output, "rqlite") {
		t.Fatalf("expected stdout output to contain 'rqlite' service name, got: %s", output)
	}
	if !strings.Contains(output, "test-node-1") {
		t.Fatalf("expected stdout output to contain node ID, got: %s", output)
	}
}

func Test_Tracer_ReturnsConsistentName(t *testing.T) {
	shutdown, err := Setup(Config{
		Enabled: true,
		NodeID:  "node-1",
		Writer:  &bytes.Buffer{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer shutdown(context.Background())

	tr := Tracer()
	if tr == nil {
		t.Fatal("expected non-nil tracer")
	}
}
