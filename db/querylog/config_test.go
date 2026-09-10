package querylog

import (
	"os"
	"testing"
	"time"
)

func Test_New_Empty(t *testing.T) {
	ql, err := New("")
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}
	if ql != nil {
		t.Fatalf("expected nil QueryLogger for empty string, got %+v", ql)
	}
}

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
	if ql.noExpandedSQL {
		t.Fatal("stderr must have noExpandedSQL=false")
	}
	if err := ql.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func Test_New_MissingFile(t *testing.T) {
	_, err := New("/nonexistent/path/query-log.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func Test_New_FullConfig(t *testing.T) {
	// 5s = 5000000000 nanoseconds
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":5000000000,"no_expanded_sql":true}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.minDuration != 5*time.Second {
		t.Fatalf("expected 5s, got %v", ql.minDuration)
	}
	if !ql.noExpandedSQL {
		t.Fatal("expected noExpandedSQL=true")
	}
}

func Test_New_MissingMinDuration_DefaultsApplied(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"no_expanded_sql":false}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.minDuration != DefaultMinDuration {
		t.Fatalf("expected %v, got %v", DefaultMinDuration, ql.minDuration)
	}
}

func Test_New_ZeroMinDuration_DefaultsApplied(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":0}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.minDuration != DefaultMinDuration {
		t.Fatalf("expected default %v applied for 0, got %v", DefaultMinDuration, ql.minDuration)
	}
}

func Test_New_MissingNoExpandedSQL_DefaultsApplied(t *testing.T) {
	// 2s = 2000000000 nanoseconds
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":2000000000}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ql.noExpandedSQL {
		t.Fatal("expected noExpandedSQL=false (default)")
	}
	if ql.minDuration != 2*time.Second {
		t.Fatalf("expected 2s, got %v", ql.minDuration)
	}
}

func Test_New_EmptyObject_DefaultsApplied(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error for {}: %v", err)
	}
	if ql.minDuration != DefaultMinDuration {
		t.Fatalf("expected default %v, got %v", DefaultMinDuration, ql.minDuration)
	}
	if ql.noExpandedSQL {
		t.Fatal("expected noExpandedSQL=false")
	}
}

func Test_New_MalformedJSON(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{not valid`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func Test_New_UnknownField(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":10000000000,"unknown":true}`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for unknown JSON field")
	}
}

func Test_New_TrailingData(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":10000000000} extra`))
	defer os.Remove(f)

	_, err := New(f)
	if err == nil {
		t.Fatal("expected error for trailing data")
	}
}

func Test_New_NoExpandedSQL_True(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"no_expanded_sql":true}`))
	defer os.Remove(f)

	ql, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ql.noExpandedSQL {
		t.Fatal("expected noExpandedSQL=true")
	}
	if ql.minDuration != DefaultMinDuration {
		t.Fatalf("expected default %v, got %v", DefaultMinDuration, ql.minDuration)
	}
}

func mustWriteQueryLogFile(t *testing.T, content []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "query-log-*.json")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(content); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return f.Name()
}
