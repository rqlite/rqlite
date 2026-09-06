package main

import (
	"os"
	"testing"
	"time"
)

// Test_ParseQueryLog_Empty verifies that an empty string disables query logging.
func Test_ParseQueryLog_Empty(t *testing.T) {
	cfg, err := parseQueryLog("")
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config for empty string, got %+v", cfg)
	}
}

// Test_ParseQueryLog_Stderr verifies that "stderr" enables all-query logging.
func Test_ParseQueryLog_Stderr(t *testing.T) {
	cfg, err := parseQueryLog(queryLogStderr)
	if err != nil {
		t.Fatalf("unexpected error for stderr: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config for stderr")
	}
	if cfg.logger == nil {
		t.Fatal("expected non-nil logger for stderr mode")
	}
	if cfg.minDuration != 0 {
		t.Fatalf("stderr mode must have minDuration=0, got %v", cfg.minDuration)
	}
	if !cfg.expandedSQL {
		t.Fatal("stderr mode must have expandedSQL=true")
	}
	if cfg.closer == nil {
		t.Fatal("expected non-nil closer for stderr mode")
	}
	// Closer must be a no-op — calling it should not panic or close os.Stderr.
	cfg.closer()
}

// Test_ParseQueryLog_MissingFile verifies that a non-existent path returns an error.
func Test_ParseQueryLog_MissingFile(t *testing.T) {
	_, err := parseQueryLog("/nonexistent/path/query-log.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// Test_ParseQueryLog_FullJSON verifies that a complete JSON file is parsed correctly.
func Test_ParseQueryLog_FullJSON(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"5s","expanded_sql":false}`))
	defer os.Remove(f)

	cfg, err := parseQueryLog(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.minDuration != 5*time.Second {
		t.Fatalf("expected minDuration=5s, got %v", cfg.minDuration)
	}
	if cfg.expandedSQL {
		t.Fatal("expected expandedSQL=false")
	}
}

// Test_ParseQueryLog_MissingDuration verifies that a missing "duration" defaults to 10s.
func Test_ParseQueryLog_MissingDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"expanded_sql":true}`))
	defer os.Remove(f)

	cfg, err := parseQueryLog(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.minDuration != queryLogDefaultMinDuration {
		t.Fatalf("expected default minDuration=%v, got %v", queryLogDefaultMinDuration, cfg.minDuration)
	}
}

// Test_ParseQueryLog_MissingExpandedSQL verifies that a missing "expanded_sql" defaults to true.
func Test_ParseQueryLog_MissingExpandedSQL(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"2s"}`))
	defer os.Remove(f)

	cfg, err := parseQueryLog(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.expandedSQL {
		t.Fatalf("expected default expandedSQL=%v, got false", queryLogDefaultExpandedSQL)
	}
	if cfg.minDuration != 2*time.Second {
		t.Fatalf("expected minDuration=2s, got %v", cfg.minDuration)
	}
}

// Test_ParseQueryLog_BothFieldsMissing verifies that {} is rejected.
func Test_ParseQueryLog_BothFieldsMissing(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{}`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for empty JSON object {}, got nil")
	}
}

// Test_ParseQueryLog_MalformedJSON verifies that invalid JSON returns an error.
func Test_ParseQueryLog_MalformedJSON(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{not valid json`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

// Test_ParseQueryLog_UnknownField verifies that unknown JSON fields are rejected.
func Test_ParseQueryLog_UnknownField(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"10s","expanded_sql":true,"unknown_option":true}`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for unknown JSON field, got nil")
	}
}

// Test_ParseQueryLog_NegativeDuration verifies that a negative duration is rejected.
func Test_ParseQueryLog_NegativeDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"-5s","expanded_sql":true}`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for negative duration, got nil")
	}
}

// Test_ParseQueryLog_InvalidDurationString verifies that an invalid duration string is rejected.
func Test_ParseQueryLog_InvalidDurationString(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"ten seconds","expanded_sql":true}`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for invalid duration string, got nil")
	}
}

// Test_ParseQueryLog_ZeroDuration verifies that "0s" is valid and logs everything.
func Test_ParseQueryLog_ZeroDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"0s","expanded_sql":true}`))
	defer os.Remove(f)

	cfg, err := parseQueryLog(f)
	if err != nil {
		t.Fatalf("unexpected error for 0s duration: %v", err)
	}
	if cfg.minDuration != 0 {
		t.Fatalf("expected minDuration=0, got %v", cfg.minDuration)
	}
}

// Test_ParseQueryLog_TrailingData verifies that trailing data after the JSON object is rejected.
// The decoder must consume exactly one JSON object; anything after it is an error.
func Test_ParseQueryLog_TrailingData(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"10s","expanded_sql":true} extra`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for trailing data after JSON object, got nil")
	}
}

// Test_ParseQueryLog_EmptyDuration verifies that an explicit empty "duration" string is rejected.
// This is distinct from a missing duration field, which defaults to 10s.
func Test_ParseQueryLog_EmptyDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":""}`))
	defer os.Remove(f)

	_, err := parseQueryLog(f)
	if err == nil {
		t.Fatal("expected error for empty duration string, got nil")
	}
}

// Test_ParseQueryLog_ExpandedSQLFalse verifies that expanded_sql=false is honoured.
func Test_ParseQueryLog_ExpandedSQLFalse(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"expanded_sql":false}`))
	defer os.Remove(f)

	cfg, err := parseQueryLog(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.expandedSQL {
		t.Fatal("expected expandedSQL=false")
	}
	// duration should default to 10s
	if cfg.minDuration != queryLogDefaultMinDuration {
		t.Fatalf("expected default minDuration=%v, got %v", queryLogDefaultMinDuration, cfg.minDuration)
	}
}

// mustWriteQueryLogFile writes content to a temp file and returns its path.
// The test will clean it up via defer os.Remove(f).
func mustWriteQueryLogFile(t *testing.T, content []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "query-log-*.json")
	if err != nil {
		t.Fatalf("failed to create temp query-log file: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(content); err != nil {
		t.Fatalf("failed to write temp query-log file: %v", err)
	}
	return f.Name()
}
