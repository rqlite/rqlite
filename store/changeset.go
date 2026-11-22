package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/rqlite/rqlite/v10/command"
	"github.com/rqlite/rqlite/v10/command/proto"
	sql "github.com/rqlite/rqlite/v10/db"
)

// Changeset replication.
//
// When Store.ChangesetReplication is set, the Leader does not put a write
// request's SQL in the Raft log. It runs the request against its own database
// inside a transaction with a SQLite session attached, keeps the changeset the
// session recorded, rolls the transaction back, and replicates the changeset.
// Every node, the Leader included, then applies the changeset. The rows a
// request touches are decided once, on the Leader, so random(), randomblob(),
// datetime('now'), DEFAULT expressions and triggers cannot differ between nodes.
//
// A request the session extension cannot record (see db.CapturableRequest and
// db.ErrNotCapturable) is replicated as SQL text, exactly as before.
//
// Between recording and applying, nothing else may change the Leader's
// database, or the changeset would be built on state the followers will not
// have. changesetMu serializes every write request while the feature is on,
// whichever path it takes, and a Raft barrier once per term makes sure the
// FSM has applied everything the previous Leader committed. The cost is
// that write requests no longer overlap in the Raft log, so the Leader loses
// Raft's batching of concurrent writes; queued writes keep their batching
// because a queue batch is one request.

// commandMarshal is a variable so that tests can make it fail, and can run code
// between recording a changeset and applying it.
var commandMarshal = command.Marshal

// changesetApply records the request's changeset on the Leader and sends it
// through Raft. It returns ok false, with nothing done, if the request must
// take the statement path. The caller must hold s.changesetMu.
func (s *Store) changesetApply(ctx context.Context, req *proto.Request, timings, unified bool) (results []*proto.ExecuteQueryResponse, idx uint64, ok bool, err error) {
	if err := s.changesetCaughtUp(); err != nil {
		return nil, 0, true, err
	}

	var cs []byte
	if unified {
		results, cs, err = s.db.RequestCapture(ctx, req, timings)
	} else {
		results, cs, err = s.db.ExecuteCapture(ctx, req, timings)
	}
	if errors.Is(err, sql.ErrNotCapturable) {
		return nil, 0, false, nil
	}
	if err != nil {
		return results, 0, true, err
	}
	if len(cs) == 0 {
		// Nothing changed (a read-only request, a rolled-back transaction, or
		// statements that matched no rows): nothing to replicate.
		return results, s.raft.AppliedIndex(), true, nil
	}

	// The changeset is the whole sub-command: it is opaque to the log.
	b, err := commandMarshal(&proto.Command{
		Type:       proto.Command_COMMAND_TYPE_CHANGESET,
		SubCommand: cs,
	})
	if err != nil {
		return nil, 0, true, err
	}
	af := s.raft.Apply(b, s.ApplyTimeout)
	if af.Error() != nil {
		return nil, 0, true, af.Error()
	}
	r := af.Response().(*fsmGenericResponse)
	if r.error != nil {
		return nil, af.Index(), true, fmt.Errorf("changeset apply: %w", r.error)
	}
	return results, af.Index(), true, nil
}

// changesetCaughtUp makes sure the FSM has applied every entry committed before
// this Leader's term began, once per term. After that, changesetMu keeps this
// Leader's own writes in step.
func (s *Store) changesetCaughtUp() error {
	term := s.raft.CurrentTerm()
	if s.changesetBarrierAt.Load() == term {
		return nil
	}
	if err := s.Barrier(); err != nil {
		return err
	}
	s.changesetBarrierAt.Store(term)
	return nil
}
