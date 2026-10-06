package store

import (
	"log"
	"path/filepath"
	"regexp"

	"github.com/rqlite/rqlite/v10/db/querylog"
)

// Config represents the configuration of the underlying Store.
type Config struct {
	DBConf *DBConfig   // The DBConfig object for this Store.
	Dir    string      // The working directory for raft.
	Tn     Transport   // The underlying Transport for raft.
	ID     string      // Node ID.
	Logger *log.Logger // The logger to use to log stuff.
	CDC    *CDCConfig  // If non-nil, Change Data Capture is enabled with this configuration.
}

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

// CDCConfig is the configuration for Change Data Capture. CDC is enabled, or
// not, for the life of a Store.
type CDCConfig struct {
	// TableRe, if non-nil, restricts CDC to tables whose names match it.
	TableRe *regexp.Regexp

	// RowIDsOnly, if true, means CDC events contain row IDs but no row data.
	RowIDsOnly bool
}
