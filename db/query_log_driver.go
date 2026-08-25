package db

import (
	"database/sql"
	"sync"

	"github.com/mattn/go-sqlite3"
)

const queryLogDriverName = "rqlite-sqlite3-querylog"

var queryLogDriverOnce sync.Once

// QueryLogDriver returns the query-log driver. It registers the SQLite3
// driver with query logging support. It can be called multiple times but
// only registers the driver once. The driver disables checkpoint-on-close
// and installs the given QueryLogger's TraceHook on every new connection.
func QueryLogDriver(ql *QueryLogger) *Driver {
	queryLogDriverOnce.Do(func() {
		sql.Register(queryLogDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: makeQueryLogConnectHookFn(ql),
		})
	})
	return &Driver{
		name:       queryLogDriverName,
		chkOnClose: CnkOnCloseModeDisabled,
	}
}

// newTestQueryLogDriver registers a query-log driver with the given name.
// It is used by tests to create isolated drivers.
func newTestQueryLogDriver(name string, ql *QueryLogger) *Driver {
	sql.Register(name, &sqlite3.SQLiteDriver{
		ConnectHook: makeQueryLogConnectHookFn(ql),
	})
	return &Driver{
		name:       name,
		chkOnClose: CnkOnCloseModeDisabled,
	}
}

// makeQueryLogConnectHookFn creates a ConnectHook that:
// 1. Disables checkpoint-on-close (same as the default driver).
// 2. Installs the QueryLogger's TraceHook via SetTrace.
func makeQueryLogConnectHookFn(ql *QueryLogger) func(conn *sqlite3.SQLiteConn) error {
	return func(conn *sqlite3.SQLiteConn) error {
		if err := conn.DBConfigNoCkptOnClose(); err != nil {
			return err
		}

		if ql != nil {
			if err := conn.SetTrace(&sqlite3.TraceConfig{
				Callback:        ql.TraceHook,
				EventMask:       sqlite3.TraceStmt | sqlite3.TraceProfile,
				WantExpandedSQL: true,
			}); err != nil {
				return err
			}
		}
		return nil
	}
}
