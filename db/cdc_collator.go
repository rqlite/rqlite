package db

import (
	"fmt"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

// CDCCollator gathers Change Data Capture events into a slice owned by the
// caller. It implements the SQLite preupdate, commit, and rollback hooks.
//
// Before modifying the database, the caller passes a pointer to a slice to
// Reset. The preupdate hook records each changed row for the transaction in
// progress. When that transaction commits, its events are appended to the
// slice. When it rolls back, they are discarded. A database change entry may
// contain several autocommit transactions, so the slice may accumulate events
// from several commits. When the caller knows that all changes for the entry
// are complete, it reads the slice it passed in, and then calls Reset with nil
// to detach the collator so that no later commit can modify events that have
// already been handed on.
//
// A CDCCollator is not safe for concurrent use. The hooks run on the goroutine
// that executes the SQL, and the caller must not read the slice until that
// execution has finished.
type CDCCollator struct {
	events  *[]*command.CDCEvent
	pending []*command.CDCEvent
	db      ColumnsNameProvider
}

// NewCDCCollator returns a collator that resolves column names through db.
// The collator starts detached, so it discards committed events until Reset
// is called with a destination.
func NewCDCCollator(db ColumnsNameProvider) (*CDCCollator, error) {
	if db == nil {
		return nil, fmt.Errorf("nil ColumnsNameProvider")
	}
	return &CDCCollator{db: db}, nil
}

// Reset makes events the destination for the events of transactions that
// commit from now on, and discards events from any transaction still in
// progress. The collator only ever appends to the slice and never clears it,
// so the caller should pass a pointer to an empty slice. Passing nil detaches
// the collator, and committed events are then discarded until the next Reset.
func (c *CDCCollator) Reset(events *[]*command.CDCEvent) {
	c.events = events
	c.pending = nil
}

// PreupdateHook records a change made by the transaction in progress. The
// event does not reach the caller's slice until that transaction commits.
func (c *CDCCollator) PreupdateHook(ev *command.CDCEvent) error {
	c.pending = append(c.pending, ev)
	return nil
}

// RollbackHook discards the events of the transaction in progress. Events
// already appended to the caller's slice by earlier commits are unaffected.
func (c *CDCCollator) RollbackHook() {
	c.pending = nil
}

// CommitHook appends the events of the transaction in progress to the caller's
// slice. Column names are resolved here rather than later, because a
// subsequent transaction in the same change entry may alter the table. It
// always returns true, because CDC bookkeeping must never cause a transaction
// to be rolled back.
func (c *CDCCollator) CommitHook() bool {
	events := c.pending
	c.pending = nil
	if len(events) == 0 || c.events == nil {
		// Schema changes commit without producing events, and a detached
		// collator has nowhere to put them.
		return true
	}

	colNames := make(map[string][]string)
	for _, ev := range events {
		if _, ok := colNames[ev.Table]; !ok {
			names, err := c.db.ColumnNames(ev.Table)
			if err != nil {
				ev.Error = fmt.Sprintf("failed to get column names for table %s: %v", ev.Table, err)
				continue
			}
			colNames[ev.Table] = names
		}
		ev.ColumnNames = colNames[ev.Table]
	}

	*c.events = append(*c.events, events...)
	return true
}

// Len returns the number of events recorded for the transaction in progress.
func (c *CDCCollator) Len() int {
	return len(c.pending)
}
