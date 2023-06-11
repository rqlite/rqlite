package store

import (
	"bytes"
	"compress/gzip"
	"expvar"
	"log"
	"math"
	"time"

	"github.com/hashicorp/raft"
	sql "github.com/rqlite/rqlite/db"
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
		b := new(bytes.Buffer)

		// Flag compressed database by writing max uint64 value first.
		// No SQLite database written by earlier versions will have this
		// as a size. *Surely*.
		err := writeUint64(b, math.MaxUint64)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b.Bytes()); err != nil {
			return err
		}
		b.Reset() // Clear state of buffer for future use.

		// Get compressed copy of database.
		cdb, err := f.compressedDatabase()
		if err != nil {
			return err
		}

		if cdb != nil {
			// Write size of compressed database.
			err = writeUint64(b, uint64(len(cdb)))
			if err != nil {
				return err
			}
			if _, err := sink.Write(b.Bytes()); err != nil {
				return err
			}

			// Write compressed database to sink.
			if _, err := sink.Write(cdb); err != nil {
				return err
			}
			stats.Get(snapshotDBOnDiskSize).(*expvar.Int).Set(int64(len(cdb)))
		} else {
			f.logger.Println("no database data available for snapshot")
			err = writeUint64(b, uint64(0))
			if err != nil {
				return err
			}
			if _, err := sink.Write(b.Bytes()); err != nil {
				return err
			}
			stats.Get(snapshotDBOnDiskSize).(*expvar.Int).Set(0)
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *FSMSnapshot) compressedDatabase() ([]byte, error) {
	if f.database == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := gz.Write(f.database); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Release is a no-op.
func (f *FSMSnapshot) Release() {}
