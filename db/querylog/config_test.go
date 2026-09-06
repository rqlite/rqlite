package querylog

import (
	"os"
	"testing"
	"time"
)

// Test_New_Empty verifies that an empty string disables query logging.
func Test_New_Empty(t *testing.T) {
	ql, err := New("")
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}
	if ql != nil {
		t.Fatalf("expected nil QueryLogger for empty string, got %+v", ql)
	}
}

// Test_New_Stderr verifies that "stderr" enables all-query logging.
func Test_New_Stderr(t *testing.T) {
	ql, err := New(stderrValue)
	if err != nil {
		t.Fatalf("unexpected error for stderr: %v", err)
	}
	if ql == nil {
		t.Fatal("expected non-nil QueryLogger for stderr")
	}
	if ql.logger == nil {
		t.Fatal("expected non-nil logger for stderr mode")
	}
	if ql.minDuration != 0 {
		t.Fatalf("stderr mode must have minDuration=0, got %v", ql.minDuration)
	}
	if !ql.expandedSQL {
		t.Fatal("stderr mode must have expandedSQL=true")
	}
	// Close must be a no-op.
	if err := ql.Close(); err != nil {
		t.Fatalf("Close() on stderr logger returned error: %v", err)
	}
}

// Test_New_MissingFile verifies that a non-existent path returns an error.
func Test_New_MissingFile(t *testing.T) {
	_, err := New("/nonexistent/path/query-log.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// Test_New_FullJSON verifies that a complete JSON file is parsed correctly.
func Test_New_FullJSON(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"5s","expanded_sql":false}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.minDuration != 5*time.Second {
		t.Fatalf("expected minDuration=5s, got %v", ql.minDuration)
	}
	if ql.expandedSQL {
		t.Fatal("expected expandedSQL=false")
	}
}

// Test_New_MissingDuration verifies that a missing "duration" defaults to DefaultMinDuration.
func Test_New_MissingDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"expanded_sql":true}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.minDuration != DefaultMinDuration {
		t.Fatalf("expected default minDuration=%v, got %v", DefaultMinDuration, ql.minDuration)
	}
}

// Test_New_MissingExpandedSQL verifies that a missing "expanded_sql" defaults to true.
func Test_New_MissingExpandedSQL(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"2s"}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ql.expandedSQL {
		t.Fatalf("expected default expandedSQL=%v, got false", defaultExpandedSQL)
	}
	if ql.minDuration != 2*time.Second {
		t.Fatalf("expected minDuration=2s, got %v", ql.minDuration)
	}
}

// Test_New_BothFieldsMissing verifies that {} is rejected.
func Test_New_BothFieldsMissing(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{}`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for empty JSON object {}, got nil")
	}
}

// Test_New_MalformedJSON verifies that invalid JSON returns an error.
func Test_New_MalformedJSON(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{not valid json`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

// Test_New_UnknownField verifies that unknown JSON fields are rejected.
func Test_New_UnknownField(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"10s","expanded_sql":true,"unknown_option":true}`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for unknown JSON field, got nil")
	}
}

// Test_New_NegativeDuration verifies that a negative duration is rejected.
func Test_New_NegativeDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"-5s","expanded_sql":true}`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for negative duration, got nil")
	}
}

// Test_New_InvalidDurationString verifies that an invalid duration string is rejected.
func Test_New_InvalidDurationString(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"ten seconds","expanded_sql":true}`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for invalid duration string, got nil")
	}
}

// Test_New_ZeroDuration verifies that "0s" is valid and logs everything.
func Test_New_ZeroDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"0s","expanded_sql":true}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error for 0s duration: %v", err)
	}
	if ql.minDuration != 0 {
		t.Fatalf("expected minDuration=0, got %v", ql.minDuration)
	}
}

// Test_New_TrailingData verifies that trailing data after the JSON object is rejected.
func Test_New_TrailingData(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":"10s","expanded_sql":true} extra`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for trailing data after JSON object, got nil")
	}
}

// Test_New_EmptyDuration verifies that an explicit empty "duration" string is rejected.
// This is distinct from a missing duration field, which defaults to DefaultMinDuration.
func Test_New_EmptyDuration(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"duration":""}`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for empty duration string, got nil")
	}
}

// Test_New_ExpandedSQLFalse verifies that expanded_sql=false is honoured.
func Test_New_ExpandedSQLFalse(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"expanded_sql":false}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.expandedSQL {
		t.Fatal("expected expandedSQL=false")
	}
	if ql.minDuration != DefaultMinDuration {
		t.Fatalf("expected default minDuration=%v, got %v", DefaultMinDuration, ql.minDuration)
	}
}

// mustWriteQueryLogFile writes content to a temp file and returns its path.
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
