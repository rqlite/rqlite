package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

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
func (d *Driver) connector(dsn string) *connector {
	return &connector{
		drv:      &sqlite3.SQLiteDriver{Extensions: d.cfg.Extensions},
		cfg:      &d.cfg,
		dsn:      dsn,
		settings: connSettings{busyTimeout: -1},
	}
}

// connSettings are the settings of a connection which can be changed while a
// database is open.
type connSettings struct {
	// busyTimeout is the busy timeout in milliseconds. If negative the
	// connection is left with the default busy timeout.
	busyTimeout int

	// synchronous is the synchronous mode. If nil the connection is left with
	// the mode set by the DSN.
	synchronous *SynchronousMode

	// Hooks. A nil hook means no hook of that type is installed.
	preUpdateHook func(sqlite3.SQLitePreUpdateData)
	updateHook    func(int, string, string, int64)
	commitHook    func() int
	rollbackHook  func()
}

// apply applies the settings to the given connection.
func (s *connSettings) apply(conn *sqlite3.SQLiteConn) error {
	if s.busyTimeout >= 0 {
		if _, err := conn.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", s.busyTimeout), nil); err != nil {
			return fmt.Errorf("failed to set busy timeout: %w", err)
		}
	}
	if s.synchronous != nil {
		if _, err := conn.Exec(fmt.Sprintf("PRAGMA synchronous=%s", *s.synchronous), nil); err != nil {
			return fmt.Errorf("failed to set synchronous mode to %s: %w", *s.synchronous, err)
		}
	}
	conn.RegisterPreUpdateHook(s.preUpdateHook)
	conn.RegisterUpdateHook(s.updateHook)
	conn.RegisterCommitHook(s.commitHook)
	conn.RegisterRollbackHook(s.rollbackHook)
	return nil
}

// connector opens the connections of a single database/sql connection pool.
// database/sql calls Connect whenever it needs a connection, including when it
// replaces one it has discarded, so every connection is configured here and
// nowhere else.
//
// A connection's configuration comes from three places: the DSN, the Driver
// configuration, and the settings. The first two are fixed when the connector
// is created. The settings can be changed while the database is open, and
// since the connector holds them, a replacement connection gets them too.
type connector struct {
	drv *sqlite3.SQLiteDriver
	cfg *DriverConfig
	dsn string

	mu       sync.Mutex
	settings connSettings
}

// Connect implements driver.Connector.
func (c *connector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn := conn.(*sqlite3.SQLiteConn)
	if err := c.configure(sqliteConn); err != nil {
		conn.Close()
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.settings.apply(sqliteConn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// update changes the settings, and applies them to conn, which must be a
// connection opened by this connector. Every connection opened afterwards will
// also have the changed settings. If the settings cannot be applied to conn
// they are left unchanged.
func (c *connector) update(conn *sqlite3.SQLiteConn, change func(s *connSettings)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	settings := c.settings
	change(&settings)
	if err := settings.apply(conn); err != nil {
		return err
	}
	c.settings = settings
	return nil
}

// Driver implements driver.Connector.
func (c *connector) Driver() driver.Driver {
	return c.drv
}

// configure applies the Driver configuration to the connection, in order:
// checkpoint config, foreign keys, then query tracing.
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
