package otlp

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// fakeCollector is an in-process OTLP gRPC metrics collector.
type fakeCollector struct {
	collectormetricspb.UnimplementedMetricsServiceServer

	mu   sync.Mutex
	reqs []*collectormetricspb.ExportMetricsServiceRequest
}

func (f *fakeCollector) Export(ctx context.Context, req *collectormetricspb.ExportMetricsServiceRequest) (*collectormetricspb.ExportMetricsServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return &collectormetricspb.ExportMetricsServiceResponse{}, nil
}

func (f *fakeCollector) requests() []*collectormetricspb.ExportMetricsServiceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*collectormetricspb.ExportMetricsServiceRequest(nil), f.reqs...)
}

// fakeTraceCollector is an in-process OTLP gRPC trace collector.
type fakeTraceCollector struct {
	collectortracepb.UnimplementedTraceServiceServer

	mu   sync.Mutex
	reqs []*collectortracepb.ExportTraceServiceRequest
}

func (f *fakeTraceCollector) Export(ctx context.Context, req *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return &collectortracepb.ExportTraceServiceResponse{}, nil
}

func (f *fakeTraceCollector) requests() []*collectortracepb.ExportTraceServiceRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*collectortracepb.ExportTraceServiceRequest(nil), f.reqs...)
}

// startFakeCollector starts an OTLP gRPC collector for metrics and traces
// on a random local port, returning both collectors along with their address.
func startFakeCollector(t *testing.T) (*fakeCollector, *fakeTraceCollector, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %s", err)
	}
	fc := &fakeCollector{}
	ftc := &fakeTraceCollector{}
	srv := grpc.NewServer()
	collectormetricspb.RegisterMetricsServiceServer(srv, fc)
	collectortracepb.RegisterTraceServiceServer(srv, ftc)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	return fc, ftc, ln.Addr().String()
}

func Test_ServiceReportsMetrics(t *testing.T) {
	m, name := newTestMap()
	m.Add("num_events", 10)
	wantMetric := "rqlite." + name + ".num_events"

	fc, _, addr := startFakeCollector(t)
	srv := NewService(Config{
		Endpoint: addr,
		Interval: 100 * time.Millisecond,
		Insecure: true,
		NodeID:   "node1",
		Version:  "v10.0.0",
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	defer srv.Stop()

	if stats, err := srv.Stats(); err != nil || stats["running"] != true {
		t.Fatalf("expected service to report running, got %v (err=%v)", stats, err)
	}

	var gotMetric, gotRuntime, gotServiceName, gotInstanceID bool
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, req := range fc.requests() {
			for _, rm := range req.GetResourceMetrics() {
				for _, attr := range rm.GetResource().GetAttributes() {
					if attr.GetKey() == "service.name" && attr.GetValue().GetStringValue() == "rqlite" {
						gotServiceName = true
					}
					if attr.GetKey() == "service.instance.id" && attr.GetValue().GetStringValue() == "node1" {
						gotInstanceID = true
					}
				}
				for _, sm := range rm.GetScopeMetrics() {
					for _, metric := range sm.GetMetrics() {
						if metric.GetName() == wantMetric {
							gotMetric = true
						}
						if strings.HasPrefix(metric.GetName(), "go.") {
							gotRuntime = true
						}
					}
				}
			}
		}
		if gotMetric && gotRuntime && gotServiceName && gotInstanceID {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !gotMetric || !gotRuntime || !gotServiceName || !gotInstanceID {
		t.Fatalf("timed out waiting for exports (metric=%v, runtime=%v, serviceName=%v, instanceID=%v)",
			gotMetric, gotRuntime, gotServiceName, gotInstanceID)
	}

	srv.Stop()
	if stats, err := srv.Stats(); err != nil || stats["running"] != false {
		t.Fatalf("expected service to report not running, got %v (err=%v)", stats, err)
	}
	srv.Stop() // Stopping a stopped service must be a no-op.
}

func Test_ServiceShutdownFlush(t *testing.T) {
	m, _ := newTestMap()
	m.Add("num_writes", 1)

	fc, _, addr := startFakeCollector(t)
	srv := NewService(Config{
		Endpoint: addr,
		Interval: time.Hour, // Ensure no periodic export happens.
		Insecure: true,
		NodeID:   "node2",
		Version:  "v10.0.0",
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	srv.Stop()

	if len(fc.requests()) == 0 {
		t.Fatalf("expected a final export on shutdown, got none")
	}
}

func Test_ServiceReportsTraces(t *testing.T) {
	_, ftc, addr := startFakeCollector(t)
	srv := NewService(Config{
		Endpoint: addr,
		Interval: time.Hour, // Ensure no periodic metric export happens.
		Insecure: true,
		NodeID:   "node3",
		Version:  "v10.0.0",
	})
	if srv.TracerProvider() != nil {
		t.Fatalf("expected nil TracerProvider before Start")
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	tp := srv.TracerProvider()
	if tp == nil {
		t.Fatalf("expected non-nil TracerProvider after Start")
	}

	_, span := tp.Tracer("test").Start(context.Background(), "test-span")
	span.End()
	srv.Stop() // Flushes any remaining spans.

	var gotSpan, gotServiceName, gotInstanceID bool
	for _, req := range ftc.requests() {
		for _, rs := range req.GetResourceSpans() {
			for _, attr := range rs.GetResource().GetAttributes() {
				if attr.GetKey() == "service.name" && attr.GetValue().GetStringValue() == "rqlite" {
					gotServiceName = true
				}
				if attr.GetKey() == "service.instance.id" && attr.GetValue().GetStringValue() == "node3" {
					gotInstanceID = true
				}
			}
			for _, ss := range rs.GetScopeSpans() {
				for _, s := range ss.GetSpans() {
					if s.GetName() == "test-span" {
						gotSpan = true
					}
				}
			}
		}
	}
	if !gotSpan || !gotServiceName || !gotInstanceID {
		t.Fatalf("expected span export on shutdown (span=%v, serviceName=%v, instanceID=%v)",
			gotSpan, gotServiceName, gotInstanceID)
	}
}

func Test_ServiceStartInvalidConfig(t *testing.T) {
	srv := NewService(Config{})
	if err := srv.Start(); err == nil {
		t.Fatalf("expected error starting service with invalid config")
	}
}
