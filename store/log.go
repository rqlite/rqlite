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

// FirstIndex returns the first index written. 0 for no entries
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
