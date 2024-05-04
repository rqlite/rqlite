package snapshot

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/command/encoding"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/rsync"
)

func Test_NewSinkCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkClose(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if sink.ID() != "snap-1234" {
		t.Fatalf("Unexpected ID: %s", sink.ID())
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to cancel unopened sink: %v", err)
	}
}

func Test_NewSinkOpenCancel(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("Failed to cancel opened sink: %v", err)
	}
}

func Test_NewSinkOpenCloseFail(t *testing.T) {
	sink := NewSink(mustStore(t), makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	if err := sink.Close(); err == nil {
		t.Fatalf("Expected error closing opened sink without data")
	}
}

// Test_SinkFullSnapshot tests that multiple full snapshots are
// written to the Store correctly. The closing of files is awkward
// on Windows, so this test is a little more involved.
func Test_SinkFullSnapshot(t *testing.T) {
	store := mustStore(t)
	sink := NewSink(store, makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	sqliteFile := mustOpenFile(t, "testdata/db-and-wals/backup.db")
	defer sqliteFile.Close()
	n, err := io.Copy(sink, sqliteFile)
	if err != nil {
		t.Fatalf("Failed to copy SQLite file: %v", err)
	}
	sqliteFile.Close() // Reaping will fail on Windows if file is not closed.
	if n != mustGetFileSize(t, "testdata/db-and-wals/backup.db") {
		t.Fatalf("Unexpected number of bytes copied: %d", n)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Check snapshot is available and correct.
	expMeta := makeRaftMeta("snap-1234", 3, 2, 1)
	metas, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(metas))
	}
	compareMetas(t, expMeta, metas[0])
	meta, fd, err := store.Open("snap-1234")
	if err != nil {
		t.Fatalf("Failed to open snapshot: %v", err)
	}
	defer fd.Close()
	compareMetas(t, expMeta, meta)
	if !compareReaderToFile(t, fd, "testdata/db-and-wals/backup.db") {
		t.Fatalf("Snapshot data does not match")
	}

	// Opening the snapshot a second time should fail due to CAS.
	_, _, errCASFail := store.Open("snap-1234")
	if errCASFail != rsync.ErrCASConflict {
		t.Fatalf("Expected CAS error opening snapshot a second time, got: %v", errCASFail)
	}
	fd.Close()

	if fn, err := store.FullNeeded(); err != nil {
		t.Fatalf("Failed to check if full snapshot needed: %v", err)
	} else if fn {
		t.Errorf("Expected full snapshot not to be needed, but it is")
	}

	// Write a second full snapshot, it should be installed without issue.
	sink = NewSink(store, makeRaftMeta("snap-5678", 4, 3, 2))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	sqliteFile2 := mustOpenFile(t, "testdata/db-and-wals/full2.db")
	defer sqliteFile2.Close()
	n, err = io.Copy(sink, sqliteFile2)
	if err != nil {
		t.Fatalf("Failed to copy second SQLite file: %v", err)
	}
	sqliteFile2.Close()
	if n != mustGetFileSize(t, "testdata/db-and-wals/full2.db") {
		t.Fatalf("Unexpected number of bytes copied: %d", n)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}

	// Check second snapshot is available and correct.
	expMeta2 := makeRaftMeta("snap-5678", 4, 3, 2)
	metas2, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(metas2) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(metas))
	}
	compareMetas(t, expMeta2, metas2[0])
	meta2, fd2, err := store.Open("snap-5678")
	if err != nil {
		t.Fatalf("Failed to open second snapshot: %v", err)
	}
	defer fd2.Close()
	compareMetas(t, expMeta2, meta2)
	if !compareReaderToFile(t, fd2, "testdata/db-and-wals/full2.db") {
		t.Fatalf("second full snapshot data does not match")
	}
	fd2.Close()

	// Check that setting FullNeeded flag works.
	if fn, err := store.FullNeeded(); err != nil {
		t.Fatalf("Failed to check if full snapshot needed: %v", err)
	} else if fn {
		t.Errorf("Expected full snapshot not to be needed, but it is")
	}

	if err := store.SetFullNeeded(); err != nil {
		t.Fatalf("Failed to set full needed: %v", err)
	}
	if fn, err := store.FullNeeded(); err != nil {
		t.Fatalf("Failed to check if full snapshot needed: %v", err)
	} else if !fn {
		t.Errorf("Expected full snapshot to be needed, but it is not")
	}

	// Write a third full snapshot, it should be installed without issue
	// and unset the FullNeeded flag.
	sink = NewSink(store, makeRaftMeta("snap-91011", 5, 4, 3))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}
	sqliteFile3 := mustOpenFile(t, "testdata/db-and-wals/full2.db")
	defer sqliteFile3.Close()
	_, err = io.Copy(sink, sqliteFile3)
	if err != nil {
		t.Fatalf("Failed to copy second SQLite file: %v", err)
	}
	sqliteFile3.Close()
	if err := sink.Close(); err != nil {
		t.Fatalf("Failed to close sink: %v", err)
	}
	if fn, err := store.FullNeeded(); err != nil {
		t.Fatalf("Failed to check if full snapshot needed: %v", err)
	} else if fn {
		t.Errorf("Expected full snapshot not to be needed, but it is")
	}

	// Make sure Store returns correct snapshot.
	expMeta3 := makeRaftMeta("snap-91011", 5, 4, 3)
	metas3, err := store.List()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(metas3) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(metas))
	}
	compareMetas(t, expMeta3, metas3[0])

	// Look inside store, make sure everything was reaped correctly.
	files, err := os.ReadDir(store.Dir())
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("Expected 2 files, got %d, %s", len(files), files)
	}
	if !fileExists(filepath.Join(store.Dir(), "snap-91011.db")) {
		t.Fatalf("Latest snapshot SQLite file does not exist")
	}
	if !dirExists(filepath.Join(store.Dir(), "snap-91011")) {
		t.Fatalf("Latest snapshot directory does not exist")
	}

}

// Test_SinkWALSnapshotEmptyStoreFail ensures that if a WAL file is
// written to empty store, an error is returned.
func Test_SinkWALSnapshotEmptyStoreFail(t *testing.T) {
	store := mustStore(t)
	sink := NewSink(store, makeRaftMeta("snap-1234", 3, 2, 1))
	if sink == nil {
		t.Fatalf("Failed to create new sink")
	}
	if err := sink.Open(); err != nil {
		t.Fatalf("Failed to open sink: %v", err)
	}

	sqliteFile := mustOpenFile(t, "testdata/db-and-wals/wal-00")
	defer sqliteFile.Close()
	n, err := io.Copy(sink, sqliteFile)
	if err != nil {
		t.Fatalf("Failed to copy SQLite file: %v", err)
	}
	if n != mustGetFileSize(t, "testdata/db-and-wals/wal-00") {
		t.Fatalf("Unexpected number of bytes copied: %d", n)
	}
	if err := sink.Close(); err == nil {
		t.Fatalf("unexpected success closing sink after writing WAL data")
	}

	// Peek inside the Store, there should be zero data inside.
	files, err := os.ReadDir(store.Dir())
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("Expected 0 files inside Store, got %d", len(files))
	}
}

// Test_SinkCreateFullThenWALSnapshots performs detailed testing of the
// snapshot creation process. It is critical that snapshots are created
// correctly, so this test is thorough.
func Test_SinkCreateFullThenWALSnapshots(t *testing.T) {
	store := mustStore(t)
	createSnapshot := func(id string, index, term, cfgIndex uint64, file string) {
		sink := NewSink(store, makeRaftMeta(id, index, term, cfgIndex))
		if sink == nil {
			t.Fatalf("Failed to create new sink")
		}
		if err := sink.Open(); err != nil {
			t.Fatalf("Failed to open sink: %v", err)
		}
		wal := mustOpenFile(t, file)
		defer wal.Close()
		_, err := io.Copy(sink, wal)
		if err != nil {
			t.Fatalf("Failed to copy WAL file: %v", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatalf("Failed to close sink: %v", err)
		}

		if fn, err := store.FullNeeded(); err != nil {
			t.Fatalf("Failed to check if full snapshot needed: %v", err)
		} else if fn {
			t.Errorf("Expected full snapshot not to be needed, but it is")
		}
	}
	if fn, err := store.FullNeeded(); err != nil {
		t.Fatalf("Failed to check if full snapshot needed: %v", err)
	} else if !fn {
		t.Errorf("Expected full snapshot to be needed, but it is not")
	}
	createSnapshot("snap-1234", 3, 2, 1, "testdata/db-and-wals/backup.db")
	createSnapshot("snap-2345", 4, 3, 2, "testdata/db-and-wals/wal-00")
	createSnapshot("snap-3456", 5, 4, 3, "testdata/db-and-wals/wal-01")
	createSnapshot("snap-4567", 6, 5, 4, "testdata/db-and-wals/wal-02")
	createSnapshot("snap-5678", 7, 6, 5, "testdata/db-and-wals/wal-03")
	createSnapshot("snap-9abc", 8, 7, 6, "testdata/db-and-wals/empty-file")

	// Check the database state inside the Store.
	dbPath, err := store.getDBPath()
	if err != nil {
		t.Fatalf("Failed to get DB path: %v", err)
	}
	if filepath.Base(dbPath) != "snap-9abc.db" {
		t.Fatalf("Unexpected DB file name: %s", dbPath)
	}
	checkDB, err := db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dbPath, err)
	}
	defer checkDB.Close()
	rows, err := checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}
}

func compareMetas(t *testing.T, m1, m2 *raft.SnapshotMeta) {
	t.Helper()
	if m1.ID != m2.ID {
		t.Fatalf("Unexpected snapshot ID: %s", m1.ID)
	}
	if m1.Index != m2.Index {
		t.Fatalf("Unexpected snapshot index: %d", m1.Index)
	}
	if m1.Term != m2.Term {
		t.Fatalf("Unexpected snapshot term: %d", m1.Term)
	}
	if m1.ConfigurationIndex != m2.ConfigurationIndex {
		t.Fatalf("Unexpected snapshot configuration index: %d", m1.ConfigurationIndex)
	}
	if m1.Version != m2.Version {
		t.Fatalf("Unexpected snapshot version: %d", m1.Version)
	}
}

func compareReaderToFile(t *testing.T, r io.Reader, path string) bool {
	t.Helper()
	fd := mustOpenFile(t, path)
	defer fd.Close()
	return compareReaderToReader(t, r, fd)
}

func compareReaderToReader(t *testing.T, r1, r2 io.Reader) bool {
	t.Helper()
	buf1, err := io.ReadAll(r1)
	if err != nil {
		t.Fatalf("Failed to read from reader 1: %v", err)
	}
	buf2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatalf("Failed to read from reader 2: %v", err)
	}
	return bytes.Equal(buf1, buf2)
}

func mustStore(t *testing.T) *Store {
	t.Helper()
	str, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	return str
}

func makeRaftMeta(id string, index, term, cfgIndex uint64) *raft.SnapshotMeta {
	return &raft.SnapshotMeta{
		ID:                 id,
		Index:              index,
		Term:               term,
		Configuration:      makeTestConfiguration("1", "localhost:1"),
		ConfigurationIndex: cfgIndex,
		Version:            1,
	}
}

func mustOpenFile(t *testing.T, path string) *os.File {
	t.Helper()
	fd, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	return fd
}

func mustGetFileSize(t *testing.T, path string) int64 {
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	return stat.Size()
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
