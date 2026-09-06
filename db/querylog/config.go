package querylog

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	// DefaultMinDuration is the minimum query duration applied when the JSON
	// configuration file omits the "duration" field.
	DefaultMinDuration = 10 * time.Second

	// stderrValue is the special flag value that enables all-query logging to stderr.
	stderrValue = "stderr"
)

// defaultExpandedSQL is applied when the JSON configuration omits "expanded_sql".
const defaultExpandedSQL = true

// Config is the JSON-loadable configuration for query logging. It represents
// the on-disk shape of the query-log configuration file. Fields are pointers
// so that absent fields can be distinguished from zero values:
//
//   - absent "duration"     → default (DefaultMinDuration)
//   - "duration": ""        → rejected (invalid)
//   - absent "expanded_sql" → default (true)
//
// Example JSON:
//
//	{"duration": "10s", "expanded_sql": true}
type Config struct {
	// Duration is the minimum execution duration before a statement is logged.
	// Go duration syntax: "10s", "500ms", "1m". Defaults to DefaultMinDuration
	// when absent. An empty string is rejected.
	Duration *string `json:"duration"`

	// ExpandedSQL controls whether the expanded SQL (with bound parameters
	// filled in) is preferred over the original statement text. Defaults to
	// true when absent.
	ExpandedSQL *bool `json:"expanded_sql"`
}

// New interprets the value of the -query-log command-line flag and returns a
// configured QueryLogger, or nil when query logging is disabled.
//
//   - ""       → disabled; returns (nil, nil)
//   - "stderr" → log every query to os.Stderr; MinDuration=0, ExpandedSQL=true
//   - <path>   → read a JSON Config from the file at path
//
// The caller must guard the returned pointer before deferring Close:
//
//	ql, err := querylog.New(cfg.QueryLog)
//	if ql != nil { defer ql.Close() }
func New(s string) (*QueryLogger, error) {
	if s == "" {
		return nil, nil
	}
	if s == stderrValue {
		return newStderrLogger(), nil
	}
	return loadFile(s)
}

// newStderrLogger returns a QueryLogger that logs every statement to stderr.
func newStderrLogger() *QueryLogger {
	return NewQueryLogger(LoggerConfig{
		Logger:      log.New(os.Stderr, "[query] ", log.LstdFlags),
		MinDuration: 0,
		ExpandedSQL: true,
	})
}

// loadFile reads and strictly validates a JSON Config file and returns a
// configured QueryLogger.
//
// Validation rules:
//   - Unknown JSON fields are rejected.
//   - Trailing data after the JSON object is rejected.
//   - An entirely empty object {} is rejected; at least one field must be present.
//   - A missing "duration" defaults to DefaultMinDuration.
//   - An explicit empty "duration": "" is rejected.
//   - A negative duration is rejected.
//   - A missing "expanded_sql" defaults to true.
func loadFile(path string) (*QueryLogger, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("query-log: cannot open config file %q: %w", path, err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("query-log: invalid JSON in config file %q: %w", path, err)
	}

	// Reject trailing data after the JSON object.
	if dec.More() {
		return nil, fmt.Errorf("query-log: config file %q contains trailing data after JSON object", path)
	}

	// Reject an entirely empty object — at least one recognised field must be present.
	if cfg.Duration == nil && cfg.ExpandedSQL == nil {
		return nil, fmt.Errorf("query-log: config file %q is empty; provide at least one of \"duration\" or \"expanded_sql\"", path)
	}

	// Resolve minDuration.
	minDuration := DefaultMinDuration
	if cfg.Duration != nil {
		if *cfg.Duration == "" {
			return nil, fmt.Errorf("query-log: \"duration\" must not be empty in %q", path)
		}
		d, err := time.ParseDuration(*cfg.Duration)
		if err != nil {
			return nil, fmt.Errorf("query-log: invalid \"duration\" value %q in %q: %w", *cfg.Duration, path, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("query-log: \"duration\" must not be negative, got %q in %q", *cfg.Duration, path)
		}
		minDuration = d
	}

	// Resolve expandedSQL.
	expandedSQL := defaultExpandedSQL
	if cfg.ExpandedSQL != nil {
		expandedSQL = *cfg.ExpandedSQL
	}

	return NewQueryLogger(LoggerConfig{
		Logger:      log.New(os.Stderr, "[query] ", log.LstdFlags),
		MinDuration: minDuration,
		ExpandedSQL: expandedSQL,
	}), nil
}
