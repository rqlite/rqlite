package store

import (
	"path/filepath"

	"github.com/rqlite/rqlite/v10/db/querylog"
)

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	// Enforce Foreign Key constraints
	FKConstraints bool `json:"fk_constraints"`

	// Paths of SQLite Extensions to be loaded
	Extensions []string `json:"extensions,omitempty"`

	// QueryLogger, if non-nil, enables query logging via the db/querylog package.
	// This field is excluded from JSON serialization as it holds runtime state.
	QueryLogger *querylog.QueryLogger `json:"-"`
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
