package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/mattn/go-sqlite3"
)

const (
	defaultDriverName = "rqlite-sqlite3"
	chkDriverName     = "rqlite-sqlite3-chk"
)

// CnkOnCloseMode represents the checkpoint on close mode.
type CnkOnCloseMode int

const (
	// CnkOnCloseModeDisabled disables checkpoint on close.
	CnkOnCloseModeDisabled CnkOnCloseMode = iota

	// CnkOnCloseModeEnabled enables checkpoint on close.
	CnkOnCloseModeEnabled
)

// DriverConfig holds the configuration for a composable SQLite driver.
type DriverConfig struct {
	// Extensions is the list of paths to SQLite extension shared objects.
	Extensions []string

	// ChkOnClose controls whether SQLite checkpoints the WAL on connection close.
	ChkOnClose CnkOnCloseMode

	// QueryLogger, if non-nil, installs query tracing on every new connection.
	QueryLogger *QueryLogger
}

// Driver is a Database driver.
type Driver struct {
	name       string
	extensions []string
	chkOnClose CnkOnCloseMode
}

// NewDriverFromConfig registers a new SQLite driver under name using cfg to
// compose the ConnectHook. Every feature in cfg is applied to each new
// connection, so extensions, checkpoint behavior, and query logging can all
// coexist.
// If a driver with name is already registered, a panic will occur. Callers
// that need a singleton driver (fixed names) should guard this with sync.Once.
func NewDriverFromConfig(name string, cfg DriverConfig) *Driver {
	sql.Register(name, &sqlite3.SQLiteDriver{
		Extensions:  cfg.Extensions,
		ConnectHook: buildConnectHook(cfg),
	})
	return &Driver{
		name:       name,
		extensions: cfg.Extensions,
		chkOnClose: cfg.ChkOnClose,
	}
}

var defRegisterOnce sync.Once

// DefaultDriver returns the default driver. It registers the SQLite3 driver
// with the default driver name. It can be called multiple times, but only
// registers the SQLite3 driver once. This driver disables checkpoint on close
// for any database in WAL mode.
func DefaultDriver() *Driver {
	defRegisterOnce.Do(func() {
		NewDriverFromConfig(defaultDriverName, DriverConfig{
			ChkOnClose: CnkOnCloseModeDisabled,
		})
	})
	return &Driver{
		name:       defaultDriverName,
		chkOnClose: CnkOnCloseModeDisabled,
	}
}

var chkRegisterOnce sync.Once

// CheckpointDriver returns the checkpoint driver. It registers the SQLite3
// driver with the checkpoint driver name. It can be called multiple times,
// but only registers the SQLite3 driver once. This driver enables checkpoint
// on close for any database in WAL mode.
func CheckpointDriver() *Driver {
	chkRegisterOnce.Do(func() {
		NewDriverFromConfig(chkDriverName, DriverConfig{
			ChkOnClose: CnkOnCloseModeEnabled,
		})
	})
	return &Driver{
		name:       chkDriverName,
		chkOnClose: CnkOnCloseModeEnabled,
	}
}

var fkRegisterOnce sync.Once

// ForeignKeyDriver returns a driver that enables foreign key support
// on every connection. It also enables no-check
func ForeignKeyDriver() *Driver {
	fkRegisterOnce.Do(func() {
		sql.Register("rqlite-sqlite3-foreignkey", &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				// Enable foreign key support via the SQLite PRAGMA
				if _, err := conn.Exec("PRAGMA foreign_keys = ON", nil); err != nil {
					return fmt.Errorf("cannot enable foreign keys: %w", err)
				}
				return nil
			},
		})
	})
	return &Driver{
		name:       "rqlite-sqlite3-foreignkey",
		chkOnClose: CnkOnCloseModeDisabled,
	}
}

// NewDriver returns a new driver with the given name and extensions. It
// registers the SQLite3 driver with the given name. extensions is a list of
// paths to SQLite3 extension shared objects. chkpt is the checkpoint-on-close
// mode the Driver will use.
//
// If a driver with the given name already exists, a panic will occur.
func NewDriver(name string, extensions []string, chkpt CnkOnCloseMode) *Driver {
	return NewDriverFromConfig(name, DriverConfig{
		Extensions: extensions,
		ChkOnClose: chkpt,
	})
}

// Name returns the driver name.
func (d *Driver) Name() string {
	return d.name
}

// Extensions returns the paths of the loaded driver extensions.
func (d *Driver) Extensions() []string {
	return d.extensions
}

// ExtensionNames returns the names of the loaded driver extensions.
func (d *Driver) ExtensionNames() []string {
	names := make([]string, 0, len(d.extensions))
	for _, ext := range d.extensions {
		names = append(names, filepath.Base(ext))
	}
	sort.Strings(names)
	return names
}

// CheckpointOnCloseMode returns the checkpoint on close mode.
func (d *Driver) CheckpointOnCloseMode() CnkOnCloseMode {
	return d.chkOnClose
}

// buildConnectHook composes a ConnectHook from cfg, chaining all requested
// connection-level behaviors in order: checkpoint config, then query tracing.
func buildConnectHook(cfg DriverConfig) func(conn *sqlite3.SQLiteConn) error {
	return func(conn *sqlite3.SQLiteConn) error {
		// Checkpoint-on-close configuration.
		if cfg.ChkOnClose == CnkOnCloseModeDisabled {
			if err := conn.DBConfigNoCkptOnClose(); err != nil {
				return fmt.Errorf("cannot disable checkpoint on close: %w", err)
			}
		}

		// Query tracing.
		if cfg.QueryLogger != nil {
			if err := conn.SetTrace(&sqlite3.TraceConfig{
				Callback:        cfg.QueryLogger.TraceHook,
				EventMask:       sqlite3.TraceStmt | sqlite3.TraceProfile,
				WantExpandedSQL: true,
			}); err != nil {
				return err
			}
		}

		return nil
	}
}
