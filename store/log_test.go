package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

func Test_LogNew(t *testing.T) {
	l := NewLog("/some/path")
	if l == nil {
		t.Fatal("got nil pointer for log")
	}
}

func Test_LogNewNotExist(t *testing.T) {
	path := mustTempDir()
	defer os.Remove(path)

	l := NewLog(path)
	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 0 {
		t.Fatalf("got non-zero value for first index of empty log: %d", fi)
	}

	li, err := l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 0 {
		t.Fatalf("got non-zero value for last index of empty log: %d", li)
	}
}

func Test_LogNewExistEmpty(t *testing.T) {
	path := mustTempDir()
	defer os.Remove(path)

	// Precreate an empty BoltDB store.
	bs, err := raftboltdb.NewBoltStore(filepath.Join(path, raftDBPath))
	if err != nil {
		t.Fatalf("failed to create bolt store: %s", err)
	}
	bs.Close()

	l := NewLog(path)

	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 0 {
		t.Fatalf("got non-zero value for first index of empty log: %d", fi)
	}

	li, err := l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 0 {
		t.Fatalf("got non-zero value for last index of empty log: %d", li)
	}
}

func Test_LogNewExistNotEmpty(t *testing.T) {
	path := mustTempDir()
	defer os.Remove(path)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(filepath.Join(path, raftDBPath))
	if err != nil {
		t.Fatalf("failed to create bolt store: %s", err)
	}
	for i := 4; i > 0; i-- {
		if err := bs.StoreLog(&raft.Log{
			Index: uint64(i),
		}); err != nil {
			t.Fatalf("failed to write entry to raft log: %s", err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l := NewLog(path)

	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 1 {
		t.Fatalf("got wrong value for first index of empty log: %d", fi)
	}

	li, err := l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 4 {
		t.Fatalf("got wrong value for last index of not empty log: %d", li)
	}

	lci, err := l.LastCommandIndex()
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 4 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	// Delete an entry, recheck index functionality.
	bs, err = raftboltdb.NewBoltStore(filepath.Join(path, raftDBPath))
	if err != nil {
		t.Fatalf("failed to re-open bolt store: %s", err)
	}
	if err := bs.DeleteRange(1, 1); err != nil {
		t.Fatalf("failed to delete range: %s", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	fi, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 2 {
		t.Fatalf("got wrong value for first index of empty log: %d", fi)
	}

	li, err = l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 4 {
		t.Fatalf("got wrong value for last index of empty log: %d", li)
	}

	fi, li, err = l.Indexes()
	if err != nil {
		t.Fatalf("failed to get indexes: %s", err)
	}
	if fi != 2 {
		t.Fatalf("got wrong value for first index of empty log: %d", fi)
	}
	if li != 4 {
		t.Fatalf("got wrong value for last index of empty log: %d", li)
	}
}

func Test_LogLastCommandIndex(t *testing.T) {
	path := mustTempDir()
	defer os.Remove(path)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(filepath.Join(path, raftDBPath))
	if err != nil {
		t.Fatalf("failed to create bolt store: %s", err)
	}

	for i := 1; i < 3; i++ {
		if err := bs.StoreLog(&raft.Log{
			Index: uint64(i),
			Type:  raft.LogCommand,
		}); err != nil {
			t.Fatalf("failed to write entry to raft log: %s", err)
		}
	}
	if err := bs.StoreLog(&raft.Log{
		Index: uint64(3),
		Type:  raft.LogNoop,
	}); err != nil {
		t.Fatalf("failed to write entry to raft log: %s", err)
	}

	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l := NewLog(path)
	lci, err := l.LastCommandIndex()
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 2 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	fi, li, err := l.Indexes()
	if err != nil {
		t.Fatalf("failed to get indexes: %s", err)
	}
	if fi != 1 {
		t.Fatalf("got wrong value for first index of empty log: %d", fi)
	}
	if li != 3 {
		t.Fatalf("got wrong for last index of empty log: %d", li)
	}
}

func Test_LogLastCommandIndexNotExist(t *testing.T) {
	path := mustTempDir()
	defer os.Remove(path)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(filepath.Join(path, raftDBPath))
	if err != nil {
		t.Fatalf("failed to create bolt store: %s", err)
	}
	for i := 4; i > 0; i-- {
		if err := bs.StoreLog(&raft.Log{
			Index: uint64(i),
			Type:  raft.LogNoop,
		}); err != nil {
			t.Fatalf("failed to write entry to raft log: %s", err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l := NewLog(path)

	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 1 {
		t.Fatalf("got wrong value for first index of empty log: %d", fi)
	}

	li, err := l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 4 {
		t.Fatalf("got wrong for last index of not empty log: %d", li)
	}

	lci, err := l.LastCommandIndex()
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 0 {
		t.Fatalf("got wrong for last command index of not empty log: %d", lci)
	}

	// Delete first log.
	bs, err = raftboltdb.NewBoltStore(filepath.Join(path, raftDBPath))
	if err != nil {
		t.Fatalf("failed to re-open bolt store: %s", err)
	}
	if err := bs.DeleteRange(1, 1); err != nil {
		t.Fatalf("failed to delete range: %s", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	lci, err = l.LastCommandIndex()
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 0 {
		t.Fatalf("got wrong for last command index of not empty log: %d", lci)
	}

}
