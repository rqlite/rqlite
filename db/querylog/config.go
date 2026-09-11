package querylog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	// DefaultMinDuration is used when "min_duration" is unset or zero.
	DefaultMinDuration = 10 * time.Second

	stderrValue = "stderr"
)

// Config holds the query-log configuration. It is both the JSON config shape
// and the argument to NewQueryLogger.
type Config struct {
	// MinDuration is the minimum query duration to log. Zero logs everything.
	// If unset, DefaultMinDuration is used.
	MinDuration time.Duration `json:"min_duration"`

	// NoExpandedSQL uses the original SQL text instead of expanded SQL.
	NoExpandedSQL bool `json:"no_expanded_sql,omitempty"`

	// Logger is the log destination. Not read from JSON; nil disables logging.
	Logger *log.Logger `json:"-"`
}

// DefaultConfig returns a Config with defaults applied.
func DefaultConfig() *Config {
	return &Config{
		MinDuration:   DefaultMinDuration,
		NoExpandedSQL: false,
	}
}

// New creates a QueryLogger from the -query-log flag value.
//   - ""       → disabled (nil, nil)
//   - "stderr" → log all queries to stderr
//   - <path>   → load JSON config from file
func New(s string) (*QueryLogger, error) {
	if s == "" {
		return nil, nil
	}
	if s == stderrValue {
		cfg := DefaultConfig()
		cfg.MinDuration = 0
		cfg.Logger = log.New(os.Stderr, "[query] ", log.LstdFlags)
		return NewQueryLogger(*cfg), nil
	}
	return newFromFile(s)
}

// newFromFile reads a JSON Config file and returns a QueryLogger.
func newFromFile(path string) (*QueryLogger, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("query-log: cannot open config file %q: %w", path, err)
	}

	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("query-log: invalid JSON in config file %q: %w", path, err)
	}

	if dec.More() {
		return nil, fmt.Errorf("query-log: config file %q contains trailing data", path)
	}

	if cfg.MinDuration == 0 {
		cfg.MinDuration = DefaultMinDuration
	}

	cfg.Logger = log.New(os.Stderr, "[query] ", log.LstdFlags)
	return NewQueryLogger(cfg), nil
}
