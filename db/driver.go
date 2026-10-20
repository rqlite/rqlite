package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db/querylog"
	"github.com/rqlite/rqlite/v10/internal/rsync"
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

	// ForeignKeys, if true, enables foreign key constraints on every new
	// connection, regardless of any setting in the DSN.
	ForeignKeys bool

	// Hooks, installed on every new connection. A nil hook is not installed.
	// PreUpdateTableRe, if non-nil, restricts PreUpdateHook to rows of tables
	// whose names match it, and PreUpdateRowIDsOnly, if true, means the events
	// passed to PreUpdateHook contain row IDs but no row data.
	PreUpdateHook       PreUpdateHookCallback
	PreUpdateTableRe    *regexp.Regexp
	PreUpdateRowIDsOnly bool
	UpdateHook          UpdateHookCallback
	CommitHook          CommitHookCallback
	RollbackHook        RollbackHookCallback

	// QueryLogger, if non-nil, installs query tracing on every new connection.
	QueryLogger *querylog.QueryLogger
}

// Driver opens SQLite connections. Every connection it opens gets the same
// extensions and the same connect hook, so a Driver is one fixed set of
// per-connection behavior. A Driver is not registered with database/sql, so
// any number can exist and none needs a name.
type Driver struct {
	sqlDrv     *sqlite3.SQLiteDriver
	extensions []string
	chkOnClose CnkOnCloseMode
}

// NewDriverFromConfig returns a new driver which uses cfg to compose the
// ConnectHook. Every feature in cfg is applied to each new connection, so
// extensions, checkpoint behavior, and query logging can all coexist.
func NewDriverFromConfig(cfg *DriverConfig) *Driver {
	return &Driver{
		sqlDrv: &sqlite3.SQLiteDriver{
			Extensions:  cfg.Extensions,
			ConnectHook: buildConnectHook(cfg),
		},
		extensions: cfg.Extensions,
		chkOnClose: cfg.ChkOnClose,
	}
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

// connector returns a database/sql connector which opens connections to the
// database at dsn using this driver.
func (d *Driver) connector(dsn string) driver.Connector {
	return newConnector(d.sqlDrv, dsn)
}

// connector opens connections to one DSN through one SQLite driver. It is
// what lets a database be opened without registering a driver name.
type connector struct {
	drv *sqlite3.SQLiteDriver
	dsn string
}

func newConnector(drv *sqlite3.SQLiteDriver, dsn string) *connector {
	return &connector{drv: drv, dsn: dsn}
}

// Connect opens a new connection to the DSN.
func (c *connector) Connect(context.Context) (driver.Conn, error) {
	return c.drv.Open(c.dsn)
}

// Driver returns the SQLite driver the connector opens connections with.
func (c *connector) Driver() driver.Driver {
	return c.drv
}

// buildConnectHook composes a ConnectHook from cfg, chaining all requested
// connection-level behaviors in order: checkpoint config, foreign keys, query
// tracing, then hooks.
//
// This driver unconditionally disables automatic checkpointing.
func buildConnectHook(cfg *DriverConfig) func(conn *sqlite3.SQLiteConn) error {
	return func(conn *sqlite3.SQLiteConn) error {
		// Checkpoint-on-close configuration.
		if cfg.ChkOnClose == CnkOnCloseModeDisabled {
			if err := conn.DBConfigNoCkptOnClose(); err != nil {
				return fmt.Errorf("cannot disable checkpoint on close: %w", err)
			}
		}

		// It's critical that rqlite has full control over the checkpointing process
		// so disable all auto-checkpointing. This doesn't return an error on a read-only
		// connection, so an error here really is an issue.
		if _, err := conn.Exec("PRAGMA wal_autocheckpoint=0", nil); err != nil {
			return fmt.Errorf("failed to disable automatic checkpointing: %s", err)
		}

		// Foreign key constraints.
		if cfg.ForeignKeys {
			if _, err := conn.Exec("PRAGMA foreign_keys = ON", nil); err != nil {
				return fmt.Errorf("cannot enable foreign keys: %w", err)
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

		// Hooks.
		if cfg.PreUpdateHook != nil {
			conn.RegisterPreUpdateHook(cfg.PreUpdateHook.SQLite(cfg.PreUpdateTableRe, cfg.PreUpdateRowIDsOnly))
		}
		if cfg.UpdateHook != nil {
			conn.RegisterUpdateHook(cfg.UpdateHook.SQLite())
		}
		if cfg.CommitHook != nil {
			conn.RegisterCommitHook(cfg.CommitHook.SQLite())
		}
		if cfg.RollbackHook != nil {
			conn.RegisterRollbackHook(cfg.RollbackHook)
		}

		return nil
	}
}

// PreUpdateHookCallback is a callback function that is called before a row is modified
// in the database.
type PreUpdateHookCallback func(ev *command.CDCEvent) error

// SQLite returns the SQLite preupdate hook which converts the SQLite hook data to rqlite
// hook data, and passes it to hook. If tblRe is non-nil only rows of tables whose names match
// it are passed to hook. If rowIDsOnly is true the events passed to hook contain row IDs but
// no row data.
func (hook *PreUpdateHookCallback) SQLite(tblRe *regexp.Regexp, rowIDsOnly bool) func(sqlite3.SQLitePreUpdateData) {
	// Convert from SQLite hook data to rqlite hook data.
	tableMatch := rsync.NewAtomicMap[string, bool]()
	convertFn := func(d sqlite3.SQLitePreUpdateData) (*command.CDCEvent, error) {
		if tblRe != nil {
			m, ok := tableMatch.Get(d.TableName)
			if !ok {
				m = tblRe.MatchString(d.TableName)
				tableMatch.Set(d.TableName, m)
			}
			if !m {
				return nil, nil
			}
		}

		ev := &command.CDCEvent{
			Table: d.TableName,
		}

		switch d.Op {
		case sqlite3.SQLITE_INSERT:
			ev.Op = command.CDCEvent_INSERT
			ev.NewRowId = d.NewRowID
		case sqlite3.SQLITE_UPDATE:
			ev.Op = command.CDCEvent_UPDATE
			ev.OldRowId = d.OldRowID
			ev.NewRowId = d.NewRowID
		case sqlite3.SQLITE_DELETE:
			ev.Op = command.CDCEvent_DELETE
			ev.OldRowId = d.OldRowID
		default:
			return ev, fmt.Errorf("unknown preupdate hook operation %d", d.Op)
		}

		// Are we done?
		if rowIDsOnly {
			return ev, nil
		}

		c := d.Count()
		if d.Op != sqlite3.SQLITE_INSERT {
			oldRow := make([]any, c)
			err := d.Old(oldRow...)
			if err != nil {
				return ev, fmt.Errorf("failed to get old row data: %w", err)
			}
			ev.OldRow, err = normalizeCDCValues(oldRow)
			if err != nil {
				return ev, fmt.Errorf("failed to normalize old row data: %w", err)
			}
		}

		if d.Op != sqlite3.SQLITE_DELETE {
			newRow := make([]any, c)
			err := d.New(newRow...)
			if err != nil {
				return ev, fmt.Errorf("failed to get new row data: %w", err)
			}
			ev.NewRow, err = normalizeCDCValues(newRow)
			if err != nil {
				return ev, fmt.Errorf("failed to normalize new row data: %w", err)
			}
		}
		return ev, nil
	}

	return func(d sqlite3.SQLitePreUpdateData) {
		stats.Add(numPreupdates, 1)
		ev, err := convertFn(d)
		if err != nil {
			stats.Add(numPreupdatesErrors, 1)
			ev.Error = err.Error()
		}
		if ev == nil {
			return
		}
		if err := (*hook)(ev); err != nil {
			stats.Add(numPreupdatesCBErrors, 1)
		}
	}
}

// UpdateHookCallback is a callback function that is called after a row is modified
// in the database.
type UpdateHookCallback func(ev *command.UpdateHookEvent) error

// SQLite returns the SQLite update hook which converts the SQLite hook data to rqlite hook
// data, and passes it to hook.
func (hook *UpdateHookCallback) SQLite() func(int, string, string, int64) {
	// Convert from SQLite hook data to rqlite hook data.
	convertFn := func(op int, _, table string, rowID int64) (*command.UpdateHookEvent, error) {
		he := &command.UpdateHookEvent{
			Table: table,
			RowId: rowID,
		}

		switch op {
		case sqlite3.SQLITE_INSERT:
			he.Op = command.UpdateHookEvent_INSERT
		case sqlite3.SQLITE_UPDATE:
			he.Op = command.UpdateHookEvent_UPDATE
		case sqlite3.SQLITE_DELETE:
			he.Op = command.UpdateHookEvent_DELETE
		default:
			return he, fmt.Errorf("unknown update hook operation %d", op)
		}
		return he, nil
	}

	return func(op int, dbName, tblName string, rowID int64) {
		stats.Add(numUpdateHooks, 1)
		ev, err := convertFn(op, dbName, tblName, rowID)
		if err != nil {
			stats.Add(numUpdateHooksErrors, 1)
			ev.Error = err.Error()
		}
		if err := (*hook)(ev); err != nil {
			stats.Add(numUpdateHooksCBErrors, 1)
		}
	}
}

// RollbackHookCallback is called when SQLite rolls back a transaction.
type RollbackHookCallback func()

// CommitHookCallback is a callback function that is called whenever a transaction
// is committed to the database. If the callback returns true the transaction
// is committed, otherwise it is rolled back.
type CommitHookCallback func() bool

// SQLite returns the SQLite commit hook which passes control to hook.
func (hook *CommitHookCallback) SQLite() func() int {
	return func() int {
		stats.Add(numCommitHooks, 1)
		if (*hook)() {
			return 0
		}
		return 1
	}
}
