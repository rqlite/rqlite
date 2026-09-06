package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	// queryLogStderr is the special flag value that enables all-query logging to stderr.
	queryLogStderr = "stderr"

	// queryLogDefaultMinDuration is applied when JSON config omits the "duration" field.
	queryLogDefaultMinDuration = 10 * time.Second

	// queryLogDefaultExpandedSQL is applied when JSON config omits the "expanded_sql" field.
	queryLogDefaultExpandedSQL = true
)

// queryLogConfig holds the resolved query-log configuration ready for use by the Store.
type queryLogConfig struct {
	logger      *log.Logger
	minDuration time.Duration
	expandedSQL bool
	closer      func() // called on shutdown; no-op for stderr
}

// parseQueryLog interprets the value of the -query-log flag and returns a
// queryLogConfig, or nil when query logging is disabled (empty string).
//
//   - ""       → disabled; returns (nil, nil)
//   - "stderr" → log all queries to stderr
//   - <path>   → read JSON configuration from the file at path
func parseQueryLog(s string) (*queryLogConfig, error) {
	if s == "" {
		return nil, nil
	}
	if s == queryLogStderr {
		return stderrQueryLogConfig(), nil
	}
	return loadQueryLogFile(s)
}

// stderrQueryLogConfig returns a queryLogConfig that logs every query to stderr.
// MinDuration is 0 (log everything) and ExpandedSQL is true.
func stderrQueryLogConfig() *queryLogConfig {
	return &queryLogConfig{
		logger:      log.New(os.Stderr, "[query] ", log.LstdFlags),
		minDuration: 0,
		expandedSQL: true,
		closer:      func() {}, // os.Stderr is not owned by us; never close it
	}
}

// queryLogFileConfig is the JSON structure for a query-log configuration file.
// Pointer fields allow absence detection so defaults can be applied correctly.
// An explicit empty string for "duration" is rejected as invalid.
type queryLogFileConfig struct {
	Duration    *string `json:"duration"`
	ExpandedSQL *bool   `json:"expanded_sql"`
}

// loadQueryLogFile reads and strictly decodes a query-log JSON configuration
// file, applies defaults for missing fields, and returns a queryLogConfig.
//
// Rules:
//   - Unknown JSON fields are rejected.
//   - Trailing data after the JSON object is rejected.
//   - An entirely empty object {} is rejected (at least one field must be present).
//   - Missing "duration" defaults to queryLogDefaultMinDuration (10s).
//   - Missing "expanded_sql" defaults to queryLogDefaultExpandedSQL (true).
//   - Negative or invalid durations are rejected.
func loadQueryLogFile(path string) (*queryLogConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("query-log: cannot open config file %q: %w", path, err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()

	var raw queryLogFileConfig
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("query-log: invalid JSON in config file %q: %w", path, err)
	}

	// Reject trailing data after the JSON object.
	if dec.More() {
		return nil, fmt.Errorf("query-log: config file %q contains trailing data after JSON object", path)
	}

	// Reject entirely empty object — at least one recognised field must be present.
	if raw.Duration == nil && raw.ExpandedSQL == nil {
		return nil, fmt.Errorf("query-log: config file %q is empty; provide at least one of \"duration\" or \"expanded_sql\"", path)
	}

	// Resolve minDuration.
	minDuration := queryLogDefaultMinDuration
	if raw.Duration != nil {
		d, err := time.ParseDuration(*raw.Duration)
		if err != nil {
			return nil, fmt.Errorf("query-log: invalid \"duration\" value %q in %q: %w", *raw.Duration, path, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("query-log: \"duration\" must not be negative, got %q in %q", *raw.Duration, path)
		}
		minDuration = d
	}

	// Resolve expandedSQL.
	expandedSQL := queryLogDefaultExpandedSQL
	if raw.ExpandedSQL != nil {
		expandedSQL = *raw.ExpandedSQL
	}

	return &queryLogConfig{
		logger:      log.New(os.Stderr, "[query] ", log.LstdFlags),
		minDuration: minDuration,
		expandedSQL: expandedSQL,
		closer:      func() {}, // currently always writes to stderr; no file to close
	}, nil
}
