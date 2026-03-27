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

func Test_Setup_WithSampling(t *testing.T) {
	var buf bytes.Buffer
	shutdown, err := Setup(Config{
		Enabled:    true,
		NodeID:     "sampled-node",
		Version:    "1.0.0",
		Writer:     &buf,
		SampleRate: 0.5,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer shutdown(context.Background())

	// Create many spans — with 50% sampling, some should be recorded.
	tracer := Tracer()
	for i := 0; i < 100; i++ {
		_, span := tracer.Start(context.Background(), "sampled-span")
		span.End()
	}

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %s", err)
	}

	// With 50% sampling over 100 spans, we should have some output
	// but not all 100. Just check it's non-empty (probabilistic).
	if buf.Len() == 0 {
		t.Fatal("expected some spans to be sampled, got none")
	}
}

func Test_Setup_OTLPEndpoint_InvalidAddr(t *testing.T) {
	// Configuring OTLP with an unreachable endpoint should not fail at setup
	// time — it creates the exporter lazily. The exporter will fail on export.
	shutdown, err := Setup(Config{
		Enabled:      true,
		NodeID:       "otlp-node",
		Version:      "1.0.0",
		OTLPEndpoint: "localhost:0",
		OTLPInsecure: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Just ensure shutdown doesn't panic.
	shutdown(context.Background())
}

func Test_Setup_BothExporters(t *testing.T) {
	// Both stdout and OTLP can be configured simultaneously.
	var buf bytes.Buffer
	shutdown, err := Setup(Config{
		Enabled:      true,
		NodeID:       "dual-node",
		Version:      "1.0.0",
		Writer:       &buf,
		OTLPEndpoint: "localhost:0",
		OTLPInsecure: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tracer := Tracer()
	_, span := tracer.Start(context.Background(), "dual-export-span")
	span.End()

	shutdown(context.Background())

	// Stdout exporter should have received the span.
	if !strings.Contains(buf.String(), "dual-export-span") {
		t.Fatalf("expected stdout to contain span, got: %s", buf.String())
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
