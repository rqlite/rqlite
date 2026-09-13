package db

import (
	"fmt"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

// ColumnsNameProvider provides column names for a given table.
type ColumnsNameProvider interface {
	ColumnNames(table string) ([]string, error)
}

// CDCStreamer collects committed changes for a Raft request and sends them
// to a channel when Flush is called.
type CDCStreamer struct {
	pending   *command.CDCIndexedEventGroup
	committed *command.CDCIndexedEventGroup
	out       chan<- *command.CDCIndexedEventGroup
	db        ColumnsNameProvider
}

// NewCDCStreamer creates a new CDCStreamer. The out channel is used
// to send the collected events to the client. It is the caller's
// responsibility to ensure that the channel is read from, as the
// CDCStreamer will drop events if the channel is full.
func NewCDCStreamer(out chan<- *command.CDCIndexedEventGroup, db ColumnsNameProvider) (*CDCStreamer, error) {
	if out == nil {
		return nil, fmt.Errorf("nil out channel")
	}
	if db == nil {
		return nil, fmt.Errorf("nil ColumnsNameProvider")
	}

	return &CDCStreamer{
		pending: &command.CDCIndexedEventGroup{
			Events: make([]*command.CDCEvent, 0),
		},
		out: out,
		db:  db,
	}, nil
}

// Reset starts a new request at Raft index k, discarding any unflushed events.
// The caller must Flush after processing the request, before the next Reset.
func (s *CDCStreamer) Reset(k uint64) {
	s.pending = &command.CDCIndexedEventGroup{
		Events: make([]*command.CDCEvent, 0),
		Index:  k,
	}
	s.committed = nil
}

// Close closes the CDCStreamer. It closes the out channel.
func (s *CDCStreamer) Close() error {
	close(s.out)
	return nil
}

// PreupdateHook is called before the update is applied. It collects
// the event and adds it to the pending events.
func (s *CDCStreamer) PreupdateHook(ev *command.CDCEvent) error {
	s.pending.Events = append(s.pending.Events, ev)
	return nil
}

// RollbackHook discards events collected for the rolled-back transaction.
func (s *CDCStreamer) RollbackHook() {
	s.pending.Events = nil
}

// CommitHook collects the pending events at a transaction's commit. A request
// can commit multiple transactions; Flush sends them together at one Raft index.
func (s *CDCStreamer) CommitHook() bool {
	if len(s.pending.Events) == 0 {
		// No CDC events to send, but let the transaction proceed.
		// CREATE TABLE statements, for example, result in a COMMIT
		// but do not generate CDC events.
		return true
	}

	colNamesCache := make(map[string][]string)
	for _, ev := range s.pending.Events {
		if _, ok := colNamesCache[ev.Table]; !ok {
			names, err := s.db.ColumnNames(ev.Table)
			if err != nil {
				errStr := fmt.Sprintf("failed to get column names for table %s: %v", ev.Table, err)
				ev.Error = errStr
				continue
			}
			colNamesCache[ev.Table] = names
		}
		ev.ColumnNames = colNamesCache[ev.Table]
	}

	if s.committed == nil {
		s.committed = &command.CDCIndexedEventGroup{Index: s.pending.Index}
	}
	s.committed.Events = append(s.committed.Events, s.pending.Events...)
	s.committed.CommitTimestamp = time.Now().UnixMilli()
	s.pending.Events = nil
	return true
}

// Flush sends the request's committed events as a single group. Uncommitted
// events are not sent. Keeping a request together allows consumers to acknowledge
// its Raft index without losing events from subsequent commits in that request.
func (s *CDCStreamer) Flush() {
	if s.committed == nil {
		return
	}
	select {
	case s.out <- s.committed:
	default:
		stats.Add(cdcDroppedEvents, 1)
	}
	s.committed = nil
}

// Len returns the number of pending events.
func (s *CDCStreamer) Len() int {
	return len(s.pending.Events)
}
