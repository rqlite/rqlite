package db

import (
	"path/filepath"
	"sort"

	"github.com/rqlite/rqlite/v10/db/querylog"
)

// CnkOnCloseMode represents the checkpoint on close mode.
type CnkOnCloseMode int

const (
	// CnkOnCloseModeDisabled disables checkpoint on close.
	CnkOnCloseModeDisabled CnkOnCloseMode = iota

	// CnkOnCloseModeEnabled enables checkpoint on close.
	CnkOnCloseModeEnabled
)

// DriverConfig holds the configuration applied to every connection a Driver opens.
type DriverConfig struct {
	// Extensions is the list of paths to SQLite extension shared objects.
	Extensions []string

	// ChkOnClose controls whether SQLite checkpoints the WAL on connection close.
	ChkOnClose CnkOnCloseMode

	// ForeignKeys, if true, enables foreign key constraints on every new
	// connection, regardless of any setting in the DSN.
	ForeignKeys bool

	// QueryLogger, if non-nil, installs query tracing on every new connection.
	QueryLogger *querylog.QueryLogger
}

// Driver describes how every connection to a database is configured. It does
// not register anything with database/sql. Instead it supplies each connection
// pool of a database with a ConnectionFactory, and that factory is the single
// place where a connection is set up.
type Driver struct {
	cfg DriverConfig
}

// NewDriverFromConfig returns a Driver which applies every feature in cfg to
// each new connection, so extensions, checkpoint behavior, and query logging
// can all coexist.
func NewDriverFromConfig(cfg *DriverConfig) *Driver {
	return &Driver{cfg: *cfg}
}

// DefaultDriver returns the default driver. This driver disables checkpoint
// on close for any database in WAL mode.
func DefaultDriver() *Driver {
	return NewDriverFromConfig(&DriverConfig{
		ChkOnClose: CnkOnCloseModeDisabled,
	})
}

// CheckpointDriver returns the checkpoint driver. This driver enables
// checkpoint on close for any database in WAL mode.
func CheckpointDriver() *Driver {
	return NewDriverFromConfig(&DriverConfig{
		ChkOnClose: CnkOnCloseModeEnabled,
	})
}

// ForeignKeyDriver returns a driver that enables foreign key support
// on every connection. This driver disables checkpoint on close for any
// database in WAL mode.
func ForeignKeyDriver() *Driver {
	return NewDriverFromConfig(&DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		ForeignKeys: true,
	})
}

// NewDriver returns a new driver with the given extensions. extensions is a
// list of paths to SQLite3 extension shared objects. chkpt is the
// checkpoint-on-close mode the Driver will use.
func NewDriver(extensions []string, chkpt CnkOnCloseMode) *Driver {
	return NewDriverFromConfig(&DriverConfig{
		Extensions: extensions,
		ChkOnClose: chkpt,
	})
}

// Extensions returns the paths of the loaded driver extensions.
func (d *Driver) Extensions() []string {
	return d.cfg.Extensions
}

// ExtensionNames returns the names of the loaded driver extensions.
func (d *Driver) ExtensionNames() []string {
	names := make([]string, 0, len(d.cfg.Extensions))
	for _, ext := range d.cfg.Extensions {
		names = append(names, filepath.Base(ext))
	}
	sort.Strings(names)
	return names
}

// CheckpointOnCloseMode returns the checkpoint on close mode.
func (d *Driver) CheckpointOnCloseMode() CnkOnCloseMode {
	return d.cfg.ChkOnClose
}

// factory returns a ConnectionFactory which opens connections using the given
// DSN, and configures them as this Driver describes. Each connection pool of a
// database gets its own factory.
func (d *Driver) factory(dsn string) *ConnectionFactory {
	return NewConnectionFactory(dsn, &d.cfg)
}
