package db

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"
)

// rwConnector reapplies writer configuration whenever database/sql replaces a
// connection, for example after cancelling a transaction. Its state belongs to
// one DB, even when multiple databases share the same registered driver.
type rwConnector struct {
	driver *sqlite3.SQLiteDriver
	dsn    string

	mu             sync.Mutex
	autoCheckpoint int
	preUpdateHook  func(sqlite3.SQLitePreUpdateData)
	updateHook     func(int, string, string, int64)
	commitHook     func() int
	rollbackHook   func()
}

func (c *rwConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	sqliteConn := conn.(*sqlite3.SQLiteConn)

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := sqliteConn.Exec(fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", c.autoCheckpoint), nil); err != nil {
		conn.Close()
		return nil, fmt.Errorf("configure autocheckpointing: %w", err)
	}
	sqliteConn.RegisterPreUpdateHook(c.preUpdateHook)
	sqliteConn.RegisterUpdateHook(c.updateHook)
	sqliteConn.RegisterCommitHook(c.commitHook)
	sqliteConn.RegisterRollbackHook(c.rollbackHook)
	return conn, nil
}

func (c *rwConnector) Driver() driver.Driver {
	return c.driver
}
