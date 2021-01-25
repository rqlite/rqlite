package store

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft-boltdb"
)

type Log struct {
	path string
}

// NewLog returns an object that can return information about the
// Raft log.
func NewLog(dir string) *Log {
	return &Log{
		path: filepath.Join(dir, raftDBPath),
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (l *Log) FirstIndex() (uint64, error) {
	bs, err := raftboltdb.NewBoltStore(l.path)
	if err != nil {
		return 0, fmt.Errorf("new bolt store: %s", err)
	}
	defer bs.Close()
	return bs.FirstIndex()
}

// LastIndex returns the last index written. 0 for no entries.
func (l *Log) LastIndex() (uint64, error) {
	bs, err := raftboltdb.NewBoltStore(l.path)
	if err != nil {
		return 0, fmt.Errorf("new bolt store: %s", err)
	}
	defer bs.Close()
	return bs.LastIndex()
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
