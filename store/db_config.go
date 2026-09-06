package store

import (
	"log"
	"path/filepath"
	"time"
)

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	// Enforce Foreign Key constraints
	FKConstraints bool `json:"fk_constraints"`

	// Paths of SQLite Extensions to be loaded
	Extensions []string `json:"extensions,omitempty"`

	// Controls query logging. If nil, query logging is disabled.
	QueryLogger *log.Logger

	// Only queries whose execution time is >= this value are logged.
	// A value of 0 logs every traced query.
	// Ignored when QueryLogger is nil.
	QueryLogMinDuration time.Duration

	// When true, the logger prefers expanded SQL (with bound parameters filled in).
	// Ignored when QueryLogger is nil.
	QueryLogExpandedSQL bool
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig() *DBConfig {
	return &DBConfig{}
}

// ExtensionNames returns the names of the SQLite extensions.
func (c *DBConfig) ExtensionNames() []string {
	names := make([]string, 0, len(c.Extensions))
	for _, ext := range c.Extensions {
		names = append(names, filepath.Base(ext))
	}
	return names
}
