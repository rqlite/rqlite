package store

import (
	"expvar"
	"log"
	"time"

	"github.com/hashicorp/raft"
	sql "github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/snapshot"
)

// FSMSnapshot is a snapshot of the SQLite database.
type FSMSnapshot struct {
	startT time.Time
	logger *log.Logger

	database []byte
}

// NewFSMSnapshot creates a new FSMSnapshot.
func NewFSMSnapshot(db *sql.DB, logger *log.Logger) *FSMSnapshot {
	fsm := &FSMSnapshot{
		startT: time.Now(),
		logger: logger,
	}

	// The error code is not meaningful from Serialize(). The code needs to be able
	// to handle a nil byte slice being returned.
	fsm.database, _ = db.Serialize()
	return fsm
}

// Persist writes the snapshot to the given sink.
func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		dur := time.Since(f.startT)
		stats.Get(snapshotPersistDuration).(*expvar.Int).Set(dur.Milliseconds())
		f.logger.Printf("snapshot and persist took %s", dur)
	}()

	err := func() error {
		v1Snap := snapshot.NewV1Encoder(f.database)
		n, err := v1Snap.WriteTo(sink)
		if err != nil {
			return err
		}
		stats.Get(snapshotDBOnDiskSize).(*expvar.Int).Set(int64(n))
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is a no-op.
func (f *FSMSnapshot) Release() {}
