package db

import (
	"fmt"
	"time"

	command "github.com/rqlite/rqlite/v9/command/proto"
)

// ColumnsNameProvider provides column names for a given table.
type ColumnsNameProvider interface {
	ColumnNames(table string) ([]string, error)
}

// CDCStreamer is a CDC streamer that collects events and sends them
// to a channel when the commit hook is called. It is used to stream
// changes to a client.
type CDCStreamer struct {
	pending *command.CDCIndexedEventGroup
	out     chan<- *command.CDCIndexedEventGroup
	db      ColumnsNameProvider
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

// Reset resets the CDCStreamer. The K value is set to the
// current K value, and all pending events are cleared. This is used
// to reset the CDCStreamer before a new transaction is started.
func (s *CDCStreamer) Reset(k uint64) {
	s.pending = &command.CDCIndexedEventGroup{
		Events: make([]*command.CDCEvent, 0),
		Index:  k,
	}
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

// CommitHook is called after the transaction is committed. It sends the
// pending events to the out channel and clears the pending events.
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

	s.pending.CommitTimestamp = time.Now().UnixMilli()
	select {
	case s.out <- s.pending:
	default:
		stats.Add(cdcDroppedEvents, 1)
	}
	s.pending = &command.CDCIndexedEventGroup{
		Events: make([]*command.CDCEvent, 0),
	}
	return true
}

// Len returns the number of pending events.
func (s *CDCStreamer) Len() int {
	return len(s.pending.Events)
}
