package db

import command "github.com/rqlite/rqlite/v8/command/proto"

// CDCStreamer is a CDC streamer that collects events and sends them
// to a channel when the commit hook is called. It is used to stream
// changes to a client.
type CDCStreamer struct {
	pending *command.CDCEvents
	out     chan<- *command.CDCEvents
}

// NewCDCStreamer creates a new CDCStreamer. The out channel is used
// to send the collected events to the client. The channel must
// be buffered, as the CDCStreamer will block until the channel is
// read.
func NewCDCStreamer(out chan<- *command.CDCEvents) *CDCStreamer {
	return &CDCStreamer{
		pending: &command.CDCEvents{
			Events: make([]*command.CDCEvent, 0),
		},
		out: out,
	}
}

// Reset resets the CDCStreamer. The K value is set to the
// current K value, and all pending events are cleared. This is used
// to reset the CDCStreamer before a new transaction is started.
func (s *CDCStreamer) Reset(k uint64) {
	s.pending = &command.CDCEvents{
		Events: make([]*command.CDCEvent, 0),
		K:      k,
	}
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
	select {
	case s.out <- s.pending:
	default:
		stats.Add(cdcDroppedEvents, 1)
	}
	s.pending = &command.CDCEvents{
		Events: make([]*command.CDCEvent, 0),
	}
	return true
}

// Len returns the number of pending events.
func (s *CDCStreamer) Len() int {
	return len(s.pending.Events)
}
