package otlp

import (
	"context"
	"expvar"
	"strings"
	"time"

	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// scopeName is the instrumentation scope for all metrics produced by the Bridge.
const scopeName = "github.com/rqlite/rqlite/v10/otlp"

// gaugeOverrides lists gauge-style metrics whose names do not match any of
// the gauge naming heuristics in classify. Keyed by expvar map name, then key.
var gaugeOverrides = map[string]map[string]bool{
	"db.wal": {
		"compact_frames_input":  true,
		"compact_frames_output": true,
	},
	"snapshot": {
		"reap_snapshots": true,
		"reap_wals":      true,
	},
}

// unitOverrides lists metrics whose units cannot be determined from their
// names. Keyed by expvar map name, then key. Units are UCUM codes.
var unitOverrides = map[string]map[string]string{
	"db": {
		"precompact_wal_size": "By",
		"compacted_wal_size":  "By",
	},
	"snapshot": {
		"persist_size": "By",
	},
}

// Bridge converts expvar metrics to the OpenTelemetry metrics data model.
// It implements the go.opentelemetry.io/otel/sdk/metric Producer interface,
// and is safe for concurrent use.
type Bridge struct {
	startTime time.Time
}

// NewBridge returns a new Bridge. The creation time of the Bridge is used
// as the start time of all cumulative metrics it produces.
func NewBridge() *Bridge {
	return &Bridge{
		startTime: time.Now(),
	}
}

// Produce returns the current state of all expvar metrics in OpenTelemetry
// form. Each key in each top-level expvar map is emitted as a metric named
// rqlite.<map>.<key>. The expvar registry is walked on every call, so maps
// and keys registered after the Bridge is created are picked up
// automatically. It never returns an error, since doing so would suppress
// the export of all other metrics gathered in the same collection cycle.
func (b *Bridge) Produce(_ context.Context) ([]metricdata.ScopeMetrics, error) {
	now := time.Now()
	var metrics []metricdata.Metrics
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == "cmdline" || kv.Key == "memstats" {
			return
		}
		m, ok := kv.Value.(*expvar.Map)
		if !ok {
			return
		}
		m.Do(func(mkv expvar.KeyValue) {
			name := "rqlite." + kv.Key + "." + mkv.Key
			gauge, unit := classify(kv.Key, mkv.Key)
			switch v := mkv.Value.(type) {
			case *expvar.Int:
				metrics = append(metrics, newMetric(name, unit, gauge, v.Value(), b.startTime, now))
			case *expvar.Float:
				metrics = append(metrics, newMetric(name, unit, gauge, v.Value(), b.startTime, now))
			}
		})
	})
	if len(metrics) == 0 {
		return nil, nil
	}
	return []metricdata.ScopeMetrics{
		{
			Scope:   instrumentation.Scope{Name: scopeName},
			Metrics: metrics,
		},
	}, nil
}

// newMetric returns a single metric holding the given value, either as a
// gauge, or as a monotonic cumulative sum starting at startTime.
func newMetric[N int64 | float64](name, unit string, gauge bool, value N, startTime, now time.Time) metricdata.Metrics {
	m := metricdata.Metrics{
		Name: name,
		Unit: unit,
	}
	dp := metricdata.DataPoint[N]{
		Time:  now,
		Value: value,
	}
	if gauge {
		m.Data = metricdata.Gauge[N]{
			DataPoints: []metricdata.DataPoint[N]{dp},
		}
	} else {
		dp.StartTime = startTime
		m.Data = metricdata.Sum[N]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints:  []metricdata.DataPoint[N]{dp},
		}
	}
	return m
}

// classify determines whether the given expvar metric is a gauge, and what
// its unit is, based on its name. Metrics not identified as gauges are
// monotonic counters. rqlite's expvar metrics are counters unless they are
// written via Set, and those gauge-style metrics are identified here either
// by naming convention, or by an explicit entry in gaugeOverrides.
func classify(mapName, key string) (gauge bool, unit string) {
	unit = metricUnit(mapName, key)
	if gaugeOverrides[mapName][key] {
		return true, unit
	}
	switch {
	case strings.Contains(key, "duration"),
		strings.HasSuffix(key, "_size"),
		strings.HasSuffix(key, "_ratio"),
		strings.HasPrefix(key, "last_"):
		return true, unit
	}
	return false, unit
}

// metricUnit returns the UCUM unit code for the given expvar metric, or an
// empty string if the unit is unknown.
func metricUnit(mapName, key string) string {
	if u, ok := unitOverrides[mapName][key]; ok {
		return u
	}
	switch {
	case strings.Contains(key, "duration"):
		if strings.HasSuffix(key, "_us") {
			return "us"
		}
		return "ms"
	case strings.HasSuffix(key, "_ratio"):
		return "1"
	case strings.HasSuffix(key, "_bytes"):
		return "By"
	}
	return ""
}
