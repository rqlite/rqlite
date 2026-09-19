package querylog

import (
	"os"
	"testing"
	"time"
)

// --- NewConfig: special input values ---

func Test_NewConfig_Empty(t *testing.T) {
	cfg, err := NewConfig("")
	if err != nil {
		t.Fatalf("unexpected error for empty string: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil Config for empty string, got %+v", cfg)
	}
}

func Test_NewConfig_Stderr(t *testing.T) {
	cfg, err := NewConfig(stderrValue)
	if err != nil {
		t.Fatalf("unexpected error for stderr: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil Config for stderr")
	}
	if cfg.MinDuration == nil {
		t.Fatal("stderr must set a non-nil MinDuration pointer")
	}
	if *cfg.MinDuration != 0 {
		t.Fatalf("stderr must have *MinDuration==0 (log every query), got %v", *cfg.MinDuration)
	}
	if cfg.NoExpandedSQL {
		t.Fatal("stderr must have NoExpandedSQL==false")
	}
}

func Test_NewConfig_MissingFile(t *testing.T) {
	_, err := NewConfig("/nonexistent/path/query-log.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func Test_NewConfig_FullConfig(t *testing.T) {
	// 5_000_000_000 ns = 5s
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":5000000000,"no_expanded_sql":true}`))
	defer os.Remove(f)

	cfg, err := NewConfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MinDuration == nil {
		t.Fatal("expected non-nil MinDuration")
	}
	if *cfg.MinDuration != 5*time.Second {
		t.Fatalf("expected 5s, got %v", *cfg.MinDuration)
	}
	if !cfg.NoExpandedSQL {
		t.Fatal("expected NoExpandedSQL=true")
	}
}

func Test_NewConfig_OmittedMinDuration_GetsDefault(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"no_expanded_sql":false}`))
	defer os.Remove(f)

	cfg, err := NewConfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MinDuration == nil {
		t.Fatal("expected non-nil MinDuration after default applied")
	}
	if *cfg.MinDuration != DefaultMinDuration {
		t.Fatalf("expected DefaultMinDuration %v, got %v", DefaultMinDuration, *cfg.MinDuration)
	}
}
func Test_NewConfig_ExplicitZeroMinDuration_LogsEverything(t *testing.T) {
	// Explicit min_duration:0 must NOT be replaced by the default.
	// Zero means "log every query".
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":0}`))
	defer os.Remove(f)

	cfg, err := NewConfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MinDuration == nil {
		t.Fatal("expected non-nil MinDuration for explicit zero")
	}
	if *cfg.MinDuration != 0 {
		t.Fatalf("explicit min_duration:0 must be preserved, got %v", *cfg.MinDuration)
	}
}

func Test_NewConfig_EmptyObject_GetsDefaults(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{}`))
	defer os.Remove(f)

	cfg, err := NewConfig(f)
	if err != nil {
		t.Fatalf("unexpected error for {}: %v", err)
	}
	if cfg.MinDuration == nil {
		t.Fatal("expected non-nil MinDuration after defaults applied")
	}
	if *cfg.MinDuration != DefaultMinDuration {
		t.Fatalf("expected DefaultMinDuration %v, got %v", DefaultMinDuration, *cfg.MinDuration)
	}
	if cfg.NoExpandedSQL {
		t.Fatal("expected NoExpandedSQL=false")
	}
}

func Test_NewConfig_NegativeMinDuration_IsRejected(t *testing.T) {
	// -1_000_000_000 ns = -1s — must be rejected by Validate().
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":-1000000000}`))
	defer os.Remove(f)

	_, err := NewConfig(f)
	if err == nil {
		t.Fatal("expected error for negative min_duration, got nil")
	}
}

func Test_NewConfig_MalformedJSON(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{not valid`))
	defer os.Remove(f)

	_, err := NewConfig(f)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func Test_NewConfig_TrailingData(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"min_duration":10000000000} extra`))
	defer os.Remove(f)

	_, err := NewConfig(f)
	if err == nil {
		t.Fatal("expected error for trailing data after JSON object")
	}
}

func Test_NewConfig_NoExpandedSQL_True(t *testing.T) {
	f := mustWriteQueryLogFile(t, []byte(`{"no_expanded_sql":true}`))
	defer os.Remove(f)

	cfg, err := NewConfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.NoExpandedSQL {
		t.Fatal("expected NoExpandedSQL=true")
	}
	if cfg.MinDuration == nil || *cfg.MinDuration != DefaultMinDuration {
		t.Fatalf("expected DefaultMinDuration, got %v", cfg.MinDuration)
	}
}

// --- Validate ---

func Test_Config_Validate_NegativeDuration_IsRejected(t *testing.T) {
	neg := -1 * time.Second
	cfg := &Config{MinDuration: &neg}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative MinDuration, got nil")
	}
}

func Test_Config_Validate_ZeroDuration_IsValid(t *testing.T) {
	zero := time.Duration(0)
	cfg := &Config{MinDuration: &zero}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero MinDuration must be valid (log everything), got: %v", err)
	}
}

func Test_Config_Validate_PositiveDuration_IsValid(t *testing.T) {
	d := 5 * time.Second
	cfg := &Config{MinDuration: &d}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("positive MinDuration must be valid, got: %v", err)
	}
}

func Test_Config_Validate_NilDuration_IsValid(t *testing.T) {
	// Nil is valid for Validate(); defaulting happens before Validate in
	// the file-load path, but Validate itself must not require non-nil.
	cfg := &Config{}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("nil MinDuration must be valid for Validate, got: %v", err)
	}
}

// --- DefaultConfig ---

func Test_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}
	if cfg.MinDuration == nil {
		t.Fatal("DefaultConfig MinDuration must not be nil")
	}
	if *cfg.MinDuration != DefaultMinDuration {
		t.Fatalf("expected %v, got %v", DefaultMinDuration, *cfg.MinDuration)
	}
	if cfg.NoExpandedSQL {
		t.Fatal("DefaultConfig NoExpandedSQL must be false")
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
