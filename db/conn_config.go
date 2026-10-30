package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db/querylog"
	"github.com/rqlite/rqlite/v10/internal/rsync"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// CnkOnCloseMode represents the checkpoint on close mode.
type CnkOnCloseMode int

const (
	// CnkOnCloseModeDisabled disables checkpoint on close.
	CnkOnCloseModeDisabled CnkOnCloseMode = iota

	// CnkOnCloseModeEnabled enables checkpoint on close.
	CnkOnCloseModeEnabled
)

// ConnConfig is the policy applied to every connection of one role when it
// is opened: which extensions it loads, how it treats the WAL on close,
// whether it enforces foreign keys, which hooks it carries, and whether its
// statements are traced.
type ConnConfig struct {
	// Extensions is the list of paths to SQLite extension shared objects.
	// The pure-Go driver cannot load extensions, so a non-empty list makes
	// every connection attempt fail.
	Extensions []string

	// ChkOnClose controls whether SQLite checkpoints the WAL on connection close.
	ChkOnClose CnkOnCloseMode

	// ForeignKeys, if true, enables foreign key constraints on every new
	// connection.
	ForeignKeys bool

	// DisableAttach, if true, stops the connection attaching other database
	// files. SQLite treats ATTACH as a read-only statement, so a read-only
	// connection is not stopped by its mode alone.
	DisableAttach bool

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

// connector returns a database/sql connector which opens connections to the
// database at dsn with this policy. The policy is copied, so later changes
// to cfg do not reach connections opened through the connector.
func (cfg ConnConfig) connector(dsn string) driver.Connector {
	drv := &sqlite.Driver{}
	drv.RegisterConnectionHook(buildConnectHook(&cfg))
	return newConnector(drv, dsn)
}

// extensionNames returns the sorted base names of the given extension paths.
func extensionNames(paths []string) []string {
	names := make([]string, 0, len(paths))
	for _, ext := range paths {
		names = append(names, filepath.Base(ext))
	}
	sort.Strings(names)
	return names
}

// connector opens connections to one DSN through one SQLite driver. It is
// what lets a database be opened without registering a driver name.
type connector struct {
	drv *sqlite.Driver
	dsn string
}

func newConnector(drv *sqlite.Driver, dsn string) *connector {
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

// buildConnectHook composes a connection hook from cfg, chaining all requested
// connection-level behaviors in order: checkpoint config, attach limit,
// foreign keys, query tracing, then hooks.
//
// This driver unconditionally disables automatic checkpointing.
func buildConnectHook(cfg *ConnConfig) sqlite.ConnectionHookFn {
	return func(conn sqlite.ExecQuerierContext, _ string) error {
		if len(cfg.Extensions) > 0 {
			return fmt.Errorf("SQLite extensions are not supported by the modernc.org/sqlite driver")
		}
		raw, err := rawHandles(conn)
		if err != nil {
			return err
		}
		ctx := context.Background()

		// Checkpoint-on-close configuration.
		if cfg.ChkOnClose == CnkOnCloseModeDisabled {
			if err := raw.dbConfigNoCkptOnClose(); err != nil {
				return fmt.Errorf("cannot disable checkpoint on close: %w", err)
			}
		}

		// It's critical that rqlite has full control over the checkpointing process
		// so disable all auto-checkpointing. This doesn't return an error on a read-only
		// connection, so an error here really is an issue.
		if _, err := conn.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0", nil); err != nil {
			return fmt.Errorf("failed to disable automatic checkpointing: %s", err)
		}

		// With the limit on attached databases set to zero SQLite refuses
		// every ATTACH before it opens anything, at no cost to other
		// statements. DETACH then fails on its own, as nothing is attached.
		if cfg.DisableAttach {
			raw.setLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
		}

		// Foreign key constraints.
		if cfg.ForeignKeys {
			if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON", nil); err != nil {
				return fmt.Errorf("cannot enable foreign keys: %w", err)
			}
		}

		// Query tracing.
		if cfg.QueryLogger != nil {
			if err := raw.setTrace(querylog.TraceStmt|querylog.TraceProfile, cfg.QueryLogger.TraceHook); err != nil {
				return err
			}
		}

		// Hooks.
		if cfg.PreUpdateHook != nil || cfg.CommitHook != nil || cfg.RollbackHook != nil {
			hr, ok := conn.(sqlite.HookRegisterer)
			if !ok {
				return fmt.Errorf("driver connection %T cannot register hooks", conn)
			}
			if cfg.PreUpdateHook != nil {
				hr.RegisterPreUpdateHook(cfg.PreUpdateHook.SQLite(cfg.PreUpdateTableRe, cfg.PreUpdateRowIDsOnly))
			}
			if cfg.CommitHook != nil {
				hr.RegisterCommitHook(cfg.CommitHook.SQLite())
			}
			if cfg.RollbackHook != nil {
				hr.RegisterRollbackHook(sqlite.RollbackHookFn(cfg.RollbackHook))
			}
		}
		if cfg.UpdateHook != nil {
			raw.registerUpdateHook(cfg.UpdateHook.SQLite())
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
func (hook *PreUpdateHookCallback) SQLite(tblRe *regexp.Regexp, rowIDsOnly bool) sqlite.PreUpdateHookFn {
	// Convert from SQLite hook data to rqlite hook data.
	tableMatch := rsync.NewAtomicMap[string, bool]()
	convertFn := func(d *sqlite.SQLitePreUpdateData) (*command.CDCEvent, error) {
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

	return func(d sqlite.SQLitePreUpdateData) {
		stats.Add(numPreupdates, 1)
		ev, err := convertFn(&d)
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
func (hook *CommitHookCallback) SQLite() sqlite.CommitHookFn {
	return func() int32 {
		stats.Add(numCommitHooks, 1)
		if (*hook)() {
			return 0
		}
		return 1
	}
}
