package log

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/rqlite/raft-boltdb"
)

// Log is an object that can return information about the Raft log.
type Log struct {
	*raftboltdb.BoltStore
}

// NewLog returns an instantiated Log object.
func NewLog(path string) (*Log, error) {
	bs, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		return nil, fmt.Errorf("new bolt store: %s", err)
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
		return 0, fmt.Errorf("get indexes: %s", err)
	}

	// Check for empty log.
	if li == 0 {
		return 0, nil
	}

	var rl raft.Log
	for i := li; i >= fi; i-- {
		if err := l.GetLog(i, &rl); err != nil {
			return 0, fmt.Errorf("get log at index %d: %s", i, err)
		}
		if rl.Type == raft.LogCommand {
			return i, nil
		}
	}
	return 0, nil
}
