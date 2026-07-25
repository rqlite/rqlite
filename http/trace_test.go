package http

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/rqlite/rqlite/v10/proxy"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// startTracedService starts a Service with tracing enabled, recording
// spans in-memory via the returned SpanRecorder.
func startTracedService(t *testing.T) (*Service, *tracetest.SpanRecorder, string) {
	t.Helper()
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)

	sr := tracetest.NewSpanRecorder()
	s.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)
	return s, sr, fmt.Sprintf("http://%s", s.Addr().String())
}

func Test_TracedRoutes(t *testing.T) {
	_, sr, host := startTracedService(t)

	client := &http.Client{}
	resp, err := client.Get(host + "/db/query")
	if err != nil {
		t.Fatalf("failed to make request: %s", err)
	}
	resp.Body.Close()

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	span := spans[0]
	if exp, got := "GET /db/query", span.Name(); exp != got {
		t.Fatalf("expected span name %s, got %s", exp, got)
	}
	if span.SpanKind() != trace.SpanKindServer {
		t.Fatalf("expected server span, got %s", span.SpanKind())
	}
	var gotStatus bool
	for _, attr := range span.Attributes() {
		if attr.Key == "http.response.status_code" {
			gotStatus = true
			if attr.Value.AsInt64() != int64(resp.StatusCode) {
				t.Fatalf("expected status code attribute %d, got %d", resp.StatusCode, attr.Value.AsInt64())
			}
		}
	}
	if !gotStatus {
		t.Fatalf("expected span to have status code attribute")
	}
}

func Test_TracedRoutes_Untraced(t *testing.T) {
	_, sr, host := startTracedService(t)

	client := &http.Client{}
	for _, path := range []string{"/status", "/readyz", "/nodes", "/debug/vars", "/xxx"} {
		resp, err := client.Get(host + path)
		if err != nil {
			t.Fatalf("failed to make request to %s: %s", path, err)
		}
		resp.Body.Close()
	}

	if spans := sr.Ended(); len(spans) != 0 {
		t.Fatalf("expected no spans for untraced routes, got %d", len(spans))
	}
}

func Test_TracePropagation(t *testing.T) {
	_, sr, host := startTracedService(t)

	const (
		traceID = "0af7651916cd43dd8448eb211c80319c"
		spanID  = "b7ad6b7169203331"
	)
	req, err := http.NewRequest("GET", host+"/db/query", nil)
	if err != nil {
		t.Fatalf("failed to create request: %s", err)
	}
	req.Header.Set("traceparent", fmt.Sprintf("00-%s-%s-01", traceID, spanID))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to make request: %s", err)
	}
	resp.Body.Close()

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	span := spans[0]
	if exp, got := traceID, span.SpanContext().TraceID().String(); exp != got {
		t.Fatalf("expected trace ID %s, got %s", exp, got)
	}
	if exp, got := spanID, span.Parent().SpanID().String(); exp != got {
		t.Fatalf("expected parent span ID %s, got %s", exp, got)
	}
}

func Test_TraceRoute(t *testing.T) {
	for _, tt := range []struct {
		path  string
		route string
		ok    bool
	}{
		{"/db/execute", "/db/execute", true},
		{"/db/query", "/db/query", true},
		{"/db/query/", "/db/query", true},
		{"/db/request", "/db/request", true},
		{"/db/backup", "/db/backup", true},
		{"/db/load", "/db/load", true},
		{"/db/sql", "/db/sql", true},
		{"/boot", "/boot", true},
		{"/snapshot", "/snapshot", true},
		{"/reap", "/reap", true},
		{"/remove", "/remove", true},
		{"/", "", false},
		{"/console/", "", false},
		{"/status", "", false},
		{"/nodes", "", false},
		{"/leader", "", false},
		{"/readyz", "", false},
		{"/licenses", "", false},
		{"/debug/vars", "", false},
		{"/debug/pprof", "", false},
		{"/bootx", "", false},
		{"/xxx", "", false},
	} {
		route, ok := traceRoute(tt.path)
		if ok != tt.ok || route != tt.route {
			t.Fatalf("traceRoute(%s) = (%s, %v), expected (%s, %v)",
				tt.path, route, ok, tt.route, tt.ok)
		}
	}
}
