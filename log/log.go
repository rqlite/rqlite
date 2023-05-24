package log

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/rqlite/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

// Log is an object that can return information about the Raft log.
type Log struct {
	*raftboltdb.BoltStore
}

// New returns an instantiated Log object that provides access to the Raft log
// stored in a BoltDB database. It takes a path to the database file and a
// boolean flag to enable/disable the freelist sync. If the flag is set to true,
// the freelist will not be synced to disk, which can improve write performance
// but may increase the risk of data loss in the event of a crash or power loss.
// Returns an error if the BoltDB store cannot be created.
func New(path string, noFreelistSync bool) (*Log, error) {
	bs, err := raftboltdb.New(raftboltdb.Options{
		BoltOptions: &bbolt.Options{
			NoFreelistSync: noFreelistSync,
		},
		Path: path,
	})
	if err != nil {
		return nil, fmt.Errorf("new bbolt store: %s", err)
	}
	return &Log{bs}, nil
}

// Indexes returns the first and last indexes.
func (l *Log) Indexes() (uint64, uint64, error) {
	fi, err := l.FirstIndex()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get first index: %s", err)
	}
	li, err := l.LastIndex()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get last index: %s", err)
	}
	return fi, li, nil
}

// LastCommandIndex returns the index of the last Command
// log entry written to the Raft log. Returns an index of
// zero if no such log exists.
func (l *Log) LastCommandIndex() (uint64, error) {
	fi, li, err := l.Indexes()
	if err != nil {
		return 0, fmt.Errorf("failed to get indexes: %s", err)
	}

	// Check for empty log.
	if li == 0 {
		return 0, nil
	}

	var rl raft.Log
	for i := li; i >= fi; i-- {
		if err := l.GetLog(i, &rl); err != nil {
			return 0, fmt.Errorf("failed to get log at index %d: %s", i, err)
		}
		if rl.Type == raft.LogCommand {
			return i, nil
		}
	}
	return 0, nil
}

// Stats returns stats about the BBoltDB database.
func (l *Log) Stats() bbolt.Stats {
	return l.BoltStore.Stats()
}
