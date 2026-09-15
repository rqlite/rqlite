package log

import (
	"os"
	"testing"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/rqlite/raft-boltdb/v2"
)

func Test_LogNewEmpty(t *testing.T) {
	path := mustTempFile(t)

	l, err := New(path, false)
	if err != nil {
		t.Fatalf("failed to create log: %s", err)
	}
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

	lci, err := l.LastCommandIndex(fi, li)
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 0 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	f, err := l.HasCommand()
	if err != nil {
		t.Fatalf("failed to get has command: %s", err)
	}
	if f {
		t.Fatalf("got wrong value for has command of empty log: %v", f)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}
}

func Test_LogNewExistNotEmpty(t *testing.T) {
	path := mustTempFile(t)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(path)
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

	l, err := New(path, false)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
	}

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

	lci, err := l.LastCommandIndex(fi, li)
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 4 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	f, err := l.HasCommand()
	if err != nil {
		t.Fatalf("failed to get has command: %s", err)
	}
	if !f {
		t.Fatalf("got wrong value for has command of non-empty log: %v", f)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}

	// Delete an entry, recheck index functionality.
	bs, err = raftboltdb.NewBoltStore(path)
	if err != nil {
		t.Fatalf("failed to re-open bolt store: %s", err)
	}
	if err := bs.DeleteRange(1, 1); err != nil {
		t.Fatalf("failed to delete range: %s", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l, err = New(path, false)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
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

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}
}

func Test_LogNewExistNotEmptyNoFreelistSync(t *testing.T) {
	path := mustTempFile(t)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(path)
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

	l, err := New(path, true)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
	}

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

	lci, err := l.LastCommandIndex(fi, li)
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 4 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}

	// Delete an entry, recheck index functionality.
	bs, err = raftboltdb.NewBoltStore(path)
	if err != nil {
		t.Fatalf("failed to re-open bolt store: %s", err)
	}
	if err := bs.DeleteRange(1, 1); err != nil {
		t.Fatalf("failed to delete range: %s", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l, err = New(path, true)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
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

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}
}

func Test_LogDeleteAll(t *testing.T) {
	path := mustTempFile(t)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		t.Fatalf("failed to create bolt store: %s", err)
	}
	for i := 1; i < 5; i++ {
		if err := bs.StoreLog(&raft.Log{
			Index: uint64(i),
		}); err != nil {
			t.Fatalf("failed to write entry to raft log: %s", err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l, err := New(path, true)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
	}

	// Check indices.
	fi, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 1 {
		t.Fatalf("got wrong value for first index of log: %d", fi)
	}
	li, err := l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 4 {
		t.Fatalf("got wrong value for last index of log: %d", li)
	}
	lci, err := l.LastCommandIndex(fi, li)
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 4 {
		t.Fatalf("got wrong value for last command index of log: %d", lci)
	}

	if err := l.DeleteRange(1, 4); err != nil {
		t.Fatalf("failed to delete range: %s", err)
	}
	fi, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("failed to get first index: %s", err)
	}
	if fi != 0 {
		t.Fatalf("got wrong value for first index of empty log: %d", fi)
	}
	li, err = l.LastIndex()
	if err != nil {
		t.Fatalf("failed to get last index: %s", err)
	}
	if li != 0 {
		t.Fatalf("got wrong value for last index of empty log: %d", li)
	}

	f, err := l.HasCommand()
	if err != nil {
		t.Fatalf("failed to get has command: %s", err)
	}
	if f {
		t.Fatalf("got wrong value for has command of empty log: %v", f)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}
}

func Test_LogLastCommandIndexNotExist(t *testing.T) {
	path := mustTempFile(t)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(path)
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

	l, err := New(path, false)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
	}

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

	lci, err := l.LastCommandIndex(fi, li)
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 0 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}

	// Delete first log.
	bs, err = raftboltdb.NewBoltStore(path)
	if err != nil {
		t.Fatalf("failed to re-open bolt store: %s", err)
	}
	if err := bs.DeleteRange(1, 1); err != nil {
		t.Fatalf("failed to delete range: %s", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}

	l, err = New(path, false)
	if err != nil {
		t.Fatalf("failed to create new log: %s", err)
	}

	fi, li, err = l.Indexes()
	if err != nil {
		t.Fatalf("failed to get indexes: %s", err)
	}
	lci, err = l.LastCommandIndex(fi, li)
	if err != nil {
		t.Fatalf("failed to get last command index: %s", err)
	}
	if lci != 0 {
		t.Fatalf("got wrong value for last command index of not empty log: %d", lci)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("failed to close log: %s", err)
	}
}

func Test_LogStats(t *testing.T) {
	path := mustTempFile(t)

	// Write some entries directory to the BoltDB Raft store.
	bs, err := raftboltdb.NewBoltStore(path)
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

	_ = bs.Stats()
	if err := bs.Close(); err != nil {
		t.Fatalf("failed to close bolt db: %s", err)
	}
}

// mustTempFile returns a path to a temporary file. The file will
// be automatically removed when the test completes.
func mustTempFile(t *testing.T) string {
	t.Helper()
	tmpfile, err := os.CreateTemp(t.TempDir(), "rqlite-log-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}
