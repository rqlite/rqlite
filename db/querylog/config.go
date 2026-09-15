package querylog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	// DefaultMinDuration is applied when min_duration is omitted from the
	// JSON config file. An explicitly supplied zero means "log every query"
	// and is never replaced by this default.
	DefaultMinDuration = 10 * time.Second

	stderrValue = "stderr"
)

// Config holds the query-log configuration. All fields are declarative
// configuration values suitable for JSON serialisation. No runtime
// resources (loggers, mutexes, channels) are stored here.
//
// MinDuration is a pointer so the JSON decoder can distinguish an explicitly
// supplied zero ("log every query") from an absent field ("use the default").
type Config struct {
	// MinDuration is the minimum query execution time that must elapse before
	// a query is logged. A nil or omitted field defaults to DefaultMinDuration.
	// A pointer to zero logs every query regardless of execution time.
	MinDuration *time.Duration `json:"min_duration,omitempty"`

	// NoExpandedSQL, when true, logs the original SQL text instead of the
	// expanded SQL with bound parameters filled in.
	NoExpandedSQL bool `json:"no_expanded_sql,omitempty"`
}

// DefaultConfig returns a Config with sensible production defaults applied.
func DefaultConfig() *Config {
	d := DefaultMinDuration
	return &Config{
		MinDuration:   &d,
		NoExpandedSQL: false,
	}
}

// NewConfig creates a Config from the -query-log flag value.
//
//   - ""       → disabled; returns nil, nil
//   - "stderr" → log every query; MinDuration is set to a pointer to zero
//   - <path>   → load JSON config from file, apply defaults, validate
//
// The stderr shorthand deliberately bypasses the JSON defaulting step so
// that its zero MinDuration is never replaced by DefaultMinDuration.
func NewConfig(s string) (*Config, error) {
	if s == "" {
		return nil, nil
	}
	if s == stderrValue {
		zero := time.Duration(0)
		return &Config{MinDuration: &zero}, nil
	}
	return newConfigFromFile(s)
}

// Validate checks that the Config is semantically valid. It is called
// automatically by NewConfig after defaults are applied. An absent
// MinDuration (nil pointer) is always valid because defaulting has already
// been applied before Validate is called via the file path; the stderr path
// sets a non-nil pointer to zero, which is also valid.
func (c *Config) Validate() error {
	if c.MinDuration != nil && *c.MinDuration < 0 {
		return errors.New("querylog: min_duration must not be negative")
	}
	return nil
}

// newConfigFromFile reads a JSON Config file from path, applies defaults to
// omitted fields, validates the result, and returns the Config.
//
// Decoding pipeline:
//
//	decode JSON → apply defaults → Validate() → return *Config
func newConfigFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("querylog: cannot open config file %q: %w", path, err)
	}

	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("querylog: invalid JSON in config file %q: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("querylog: config file %q contains trailing data", path)
		}
		return nil, fmt.Errorf("querylog: config file %q contains invalid trailing data: %w", path, err)
	}

	// Apply defaults to omitted fields only. An explicit zero is left as-is.
	if cfg.MinDuration == nil {
		d := DefaultMinDuration
		cfg.MinDuration = &d
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
