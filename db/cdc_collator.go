package db

import (
	"fmt"
	"strings"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

// CDCCollator gathers Change Data Capture events. It implements the SQLite
// preupdate, commit, and rollback hooks.
//
// Before modifying the database, the caller calls Reset. The preupdate hook
// records each changed row for the transaction in progress. When that
// transaction commits, its events are added to the collected events. When it
// rolls back, they are discarded. A database change entry may contain several
// autocommit transactions, so events from several commits may be collected.
// When the caller knows that all changes for the entry are complete, it calls
// Events to retrieve what was collected.
//
// A CDCCollator is not safe for concurrent use. The hooks run on the goroutine
// that executes the SQL, and the caller must not call Events until that
// execution has finished.
type CDCCollator struct {
	events  []*command.CDCEvent
	pending []*command.CDCEvent
}

// NewCDCCollator returns a collator with no events collected.
func NewCDCCollator() *CDCCollator {
	return &CDCCollator{}
}

// Reset discards all collected events, and the events of any transaction
// still in progress. The collator releases its storage rather than reusing
// it, so a slice previously returned by Events is never modified.
func (c *CDCCollator) Reset() {
	c.events = nil
	c.pending = nil
}

// Events returns the events of all transactions committed since the last
// Reset, in the order they occurred. It returns nil if no events have been
// collected. The returned slice remains valid after Reset.
func (c *CDCCollator) Events() []*command.CDCEvent {
	return c.events
}

// PreupdateHook records a change made by the transaction in progress. The
// event is not returned by Events until that transaction commits.
func (c *CDCCollator) PreupdateHook(ev *command.CDCEvent) error {
	c.pending = append(c.pending, ev)
	return nil
}

// RollbackHook discards the events of the transaction in progress. Events
// collected from earlier commits are unaffected.
func (c *CDCCollator) RollbackHook() {
	c.pending = nil
}

// CommitHook adds the events of the transaction in progress to the collected
// events. Column names are resolved through conn, the connection committing
// the transaction, so they reflect the schema as that transaction sees it at
// commit time. It always returns true, because CDC bookkeeping must never
// cause a transaction to be rolled back.
func (c *CDCCollator) CommitHook(conn *sqlite3.SQLiteConn) bool {
	events := c.pending
	c.pending = nil
	if len(events) == 0 {
		// Schema changes commit without producing events.
		return true
	}

	colNames := make(map[string][]string)
	for _, ev := range events {
		if _, ok := colNames[ev.Table]; !ok {
			names, err := columnNames(conn, ev.Table)
			if err != nil {
				ev.Error = fmt.Sprintf("failed to get column names for table %s: %v", ev.Table, err)
				continue
			}
			colNames[ev.Table] = names
		}
		ev.ColumnNames = colNames[ev.Table]
	}

	c.events = append(c.events, events...)
	return true
}

// columnNames returns the names of the columns of table as seen by conn,
// including any schema change made by a transaction open on conn.
func columnNames(conn *sqlite3.SQLiteConn, table string) ([]string, error) {
	rows, err := conn.Query(fmt.Sprintf(`SELECT * FROM "%s" LIMIT 0`, strings.ReplaceAll(table, `"`, `""`)), nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows.Columns(), nil
}
