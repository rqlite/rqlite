package otlp

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
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

// startFakeCollector starts an OTLP gRPC metrics collector on a random
// local port, returning it along with its address.
func startFakeCollector(t *testing.T) (*fakeCollector, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %s", err)
	}
	fc := &fakeCollector{}
	srv := grpc.NewServer()
	collectormetricspb.RegisterMetricsServiceServer(srv, fc)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	return fc, ln.Addr().String()
}

func Test_ServiceReportsMetrics(t *testing.T) {
	m, name := newTestMap()
	m.Add("num_events", 10)
	wantMetric := "rqlite." + name + ".num_events"

	fc, addr := startFakeCollector(t)
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

	fc, addr := startFakeCollector(t)
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

func Test_ServiceStartInvalidConfig(t *testing.T) {
	srv := NewService(Config{})
	if err := srv.Start(); err == nil {
		t.Fatalf("expected error starting service with invalid config")
	}
}
