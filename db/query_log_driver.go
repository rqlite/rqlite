package db

import (
	"database/sql"

	"github.com/mattn/go-sqlite3"
)

// NewQueryLogDriver returns a new driver with query logging enabled.
// It registers a SQLite3 driver that installs the given QueryLogger's
// TraceHook on every new connection via ConnectHook. The driver also
// disables checkpoint-on-close (same as DefaultDriver).
// name must be unique across the process. If a driver with the given
// name already exists, a panic will occur — same behavior as NewDriver.
func NewQueryLogDriver(name string, ql *QueryLogger) *Driver {
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
		// Same as DefaultDriver: disable checkpoint on close.
		if err := conn.DBConfigNoCkptOnClose(); err != nil {
			return err
		}

		// Install trace callback for query logging.
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
