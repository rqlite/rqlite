package otlp

import (
	"context"
	"expvar"
	"fmt"
	"sync/atomic"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var testMapN atomic.Int64

// newTestMap registers a new expvar map with a process-unique name. The
// expvar registry is global and permanent, so each test must use its own
// map.
func newTestMap() (*expvar.Map, string) {
	name := fmt.Sprintf("otlptest_%d", testMapN.Add(1))
	return expvar.NewMap(name), name
}

func Test_Classify(t *testing.T) {
	for _, tt := range []struct {
		mapName   string
		key       string
		wantGauge bool
		wantUnit  string
	}{
		// Counters.
		{"store", "num_snapshots", false, ""},
		{"http", "executions", false, ""},
		{"uploader", "total_upload_bytes", false, "By"},
		{"proto", "num_compressed_bytes", false, "By"},

		// Gauges by naming convention.
		{"db", "checkpoint_duration_ms", true, "ms"},
		{"store", "fsm_apply_duration_us", true, "us"},
		{"store", "auto_vacuum_duration", true, "ms"},
		{"cdc.service", "fifo_size", true, ""},
		{"db.wal", "compact_frames_ratio", true, "1"},
		{"uploader", "last_upload_bytes", true, "By"},

		// Gauges with units set via unitOverrides.
		{"snapshot", "persist_size", true, "By"},
		{"db", "precompact_wal_size", true, "By"},
		{"db", "compacted_wal_size", true, "By"},

		// Gauges via gaugeOverrides.
		{"db.wal", "compact_frames_input", true, ""},
		{"db.wal", "compact_frames_output", true, ""},
		{"snapshot", "reap_snapshots", true, ""},
		{"snapshot", "reap_wals", true, ""},

		// Overrides are per-map, so the same key elsewhere is a counter.
		{"store", "reap_snapshots", false, ""},
	} {
		gauge, unit := classify(tt.mapName, tt.key)
		if gauge != tt.wantGauge {
			t.Fatalf("classify(%s, %s) gauge = %v, want %v", tt.mapName, tt.key, gauge, tt.wantGauge)
		}
		if unit != tt.wantUnit {
			t.Fatalf("classify(%s, %s) unit = %q, want %q", tt.mapName, tt.key, unit, tt.wantUnit)
		}
	}
}

func Test_BridgeProduce(t *testing.T) {
	m, name := newTestMap()
	m.Add("num_things", 5)
	m.Add("open_duration_ms", 0)
	m.Get("open_duration_ms").(*expvar.Int).Set(123)
	m.AddFloat("hit_ratio", 0.75)
	s := new(expvar.String)
	s.Set("not a metric")
	m.Set("some_string", s)

	b := NewBridge()
	sms, err := b.Produce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(sms) != 1 {
		t.Fatalf("expected 1 ScopeMetrics, got %d", len(sms))
	}
	if sms[0].Scope.Name != scopeName {
		t.Fatalf("unexpected scope name %s", sms[0].Scope.Name)
	}
	prefix := "rqlite." + name + "."

	sum := findMetric(t, sms, prefix+"num_things")
	sumData, ok := sum.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("num_things is %T, want Sum[int64]", sum.Data)
	}
	if !sumData.IsMonotonic {
		t.Fatalf("num_things should be monotonic")
	}
	if sumData.Temporality != metricdata.CumulativeTemporality {
		t.Fatalf("num_things temporality = %v, want cumulative", sumData.Temporality)
	}
	if len(sumData.DataPoints) != 1 {
		t.Fatalf("num_things has %d data points, want 1", len(sumData.DataPoints))
	}
	if v := sumData.DataPoints[0].Value; v != 5 {
		t.Fatalf("num_things = %d, want 5", v)
	}
	if sumData.DataPoints[0].StartTime.IsZero() {
		t.Fatalf("num_things has zero start time")
	}

	gauge := findMetric(t, sms, prefix+"open_duration_ms")
	gaugeData, ok := gauge.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("open_duration_ms is %T, want Gauge[int64]", gauge.Data)
	}
	if v := gaugeData.DataPoints[0].Value; v != 123 {
		t.Fatalf("open_duration_ms = %d, want 123", v)
	}
	if gauge.Unit != "ms" {
		t.Fatalf("open_duration_ms unit = %q, want ms", gauge.Unit)
	}

	fgauge := findMetric(t, sms, prefix+"hit_ratio")
	fgaugeData, ok := fgauge.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("hit_ratio is %T, want Gauge[float64]", fgauge.Data)
	}
	if v := fgaugeData.DataPoints[0].Value; v != 0.75 {
		t.Fatalf("hit_ratio = %f, want 0.75", v)
	}

	if hasMetric(sms, prefix+"some_string") {
		t.Fatalf("string-valued expvar should not be exported")
	}
	if hasMetric(sms, "rqlite.cmdline") || hasMetric(sms, "rqlite.memstats") {
		t.Fatalf("built-in expvars should not be exported")
	}

	// Counter updates must be reflected, and the start time must be stable.
	m.Add("num_things", 2)
	sms2, err := b.Produce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	sum2Data := findMetric(t, sms2, prefix+"num_things").Data.(metricdata.Sum[int64])
	if v := sum2Data.DataPoints[0].Value; v != 7 {
		t.Fatalf("num_things = %d, want 7", v)
	}
	if !sum2Data.DataPoints[0].StartTime.Equal(sumData.DataPoints[0].StartTime) {
		t.Fatalf("start time changed between Produce calls")
	}
}

func Test_BridgeManualReader(t *testing.T) {
	m, name := newTestMap()
	m.Add("num_reads", 42)

	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(NewBridge()))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect: %s", err)
	}

	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != scopeName {
			continue
		}
		for _, metric := range sm.Metrics {
			if metric.Name == "rqlite."+name+".num_reads" {
				return
			}
		}
	}
	t.Fatalf("bridged metric not found in collected ResourceMetrics")
}

// findMetric returns the named metric, failing the test if it is not present.
func findMetric(t *testing.T, sms []metricdata.ScopeMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range sms {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %s not found", name)
	return metricdata.Metrics{}
}

func hasMetric(sms []metricdata.ScopeMetrics, name string) bool {
	for _, sm := range sms {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return true
			}
		}
	}
	return false
}
