package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/mattn/go-sqlite3"
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

// Driver describes how every connection to a database is opened and
// configured. It does not register anything with database/sql. Instead it
// supplies each connection pool of a database with a connector, and that
// connector is the single place where a connection is set up.
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

// connector returns a connector which opens connections using the given DSN.
// Each connection pool of a database gets its own connector.
func (d *Driver) connector(dsn string) driver.Connector {
	return &connector{
		drv: &sqlite3.SQLiteDriver{Extensions: d.cfg.Extensions},
		cfg: &d.cfg,
		dsn: dsn,
	}
}

// connector opens the connections of a single database/sql connection pool.
// database/sql calls Connect whenever it needs a connection, including when it
// replaces one it has discarded, so every connection is configured here and
// nowhere else.
type connector struct {
	drv *sqlite3.SQLiteDriver
	cfg *DriverConfig
	dsn string
}

// Connect implements driver.Connector.
func (c *connector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	if err := c.configure(conn.(*sqlite3.SQLiteConn)); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// Driver implements driver.Connector.
func (c *connector) Driver() driver.Driver {
	return c.drv
}

// configure applies all connection-level behaviors in order: checkpoint
// config, foreign keys, then query tracing.
//
// Automatic checkpointing is unconditionally disabled.
func (c *connector) configure(conn *sqlite3.SQLiteConn) error {
	// Checkpoint-on-close configuration.
	if c.cfg.ChkOnClose == CnkOnCloseModeDisabled {
		if err := conn.DBConfigNoCkptOnClose(); err != nil {
			return fmt.Errorf("cannot disable checkpoint on close: %w", err)
		}
	}

	// It's critical that rqlite has full control over the checkpointing process
	// so disable all auto-checkpoint. This doesn't return an error on a read-only
	// connection, so an error here really is an issue.
	if _, err := conn.Exec("PRAGMA wal_autocheckpoint=0", nil); err != nil {
		return fmt.Errorf("failed to disable automatic checkpointing: %s", err)
	}

	// Foreign key constraints.
	if c.cfg.ForeignKeys {
		if _, err := conn.Exec("PRAGMA foreign_keys = ON", nil); err != nil {
			return fmt.Errorf("cannot enable foreign keys: %w", err)
		}
	}

	// Query tracing.
	if c.cfg.QueryLogger != nil {
		if err := conn.SetTrace(&sqlite3.TraceConfig{
			Callback:        c.cfg.QueryLogger.TraceHook,
			EventMask:       sqlite3.TraceStmt | sqlite3.TraceProfile,
			WantExpandedSQL: true,
		}); err != nil {
			return err
		}
	}

	return nil
}
