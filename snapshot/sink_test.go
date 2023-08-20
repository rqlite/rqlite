package snapshot

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/db"
)

func Test_NewSinkOpenCloseOK(t *testing.T) {
	tmpDir := t.TempDir()
	workDir := filepath.Join(tmpDir, "work")
	mustCreateDir(workDir)
	currGenDir := filepath.Join(tmpDir, "curr")
	nextGenDir := filepath.Join(tmpDir, "next")
	str := mustNewStoreForSinkTest(t)

	s := NewSink(str, workDir, currGenDir, nextGenDir, &Meta{})
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func Test_SinkFullSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	workDir := filepath.Join(tmpDir, "work")
	mustCreateDir(workDir)
	currGenDir := filepath.Join(tmpDir, "curr")
	nextGenDir := filepath.Join(tmpDir, "next")
	str := mustNewStoreForSinkTest(t)

	s := NewSink(str, workDir, currGenDir, nextGenDir, makeMeta("snap-1234", 3, 2, 1))
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	sqliteFile := "testdata/db-and-wals/backup.db"
	wal0 := "testdata/db-and-wals/wal-00"
	wal1 := "testdata/db-and-wals/wal-01"
	wal2 := "testdata/db-and-wals/wal-02"
	wal3 := "testdata/db-and-wals/wal-03"
	stream, err := NewFullStream(sqliteFile, wal0, wal1, wal2, wal3)
	if err != nil {
		t.Fatal(err)
	}

	if io.Copy(s, stream); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Next generation directory should exist and contain a snapshot.
	if !dirExists(nextGenDir) {
		t.Fatalf("next generation directory %s does not exist", nextGenDir)
	}
	if !dirExists(filepath.Join(nextGenDir, "snap-1234")) {
		t.Fatalf("next generation directory %s does not contain snapshot directory", nextGenDir)
	}
	if !fileExists(filepath.Join(nextGenDir, baseSqliteFile)) {
		t.Fatalf("next generation directory %s does not contain base SQLite file", nextGenDir)
	}
	expMetaPath := filepath.Join(nextGenDir, "snap-1234", metaFileName)
	if !fileExists(expMetaPath) {
		t.Fatalf("meta file does not exist at %s", expMetaPath)
	}

	// Check SQLite database has been created correctly.
	db, err := db.Open(filepath.Join(nextGenDir, baseSqliteFile), false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[4]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_SinkIncrementalSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	workDir := filepath.Join(tmpDir, "work")
	mustCreateDir(workDir)
	currGenDir := filepath.Join(tmpDir, "curr")
	mustCreateDir(currGenDir)
	nextGenDir := filepath.Join(tmpDir, "next")
	str := mustNewStoreForSinkTest(t)

	s := NewSink(str, workDir, currGenDir, nextGenDir, makeMeta("snap-1234", 3, 2, 1))
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	walData := mustReadFile("testdata/db-and-wals/wal-00")
	stream, err := NewIncrementalStream(walData)
	if err != nil {
		t.Fatal(err)
	}

	if io.Copy(s, stream); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if dirExists(nextGenDir) {
		t.Fatalf("next generation directory %s exists", nextGenDir)
	}
	if !dirExists(filepath.Join(currGenDir, "snap-1234")) {
		t.Fatalf("current generation directory %s does not contain snapshot directory", currGenDir)
	}

	expWALPath := filepath.Join(currGenDir, "snap-1234", snapWALFile)
	if !fileExists(expWALPath) {
		t.Fatalf("WAL file does not exist at %s", expWALPath)
	}
	if !bytes.Equal(walData, mustReadFile(expWALPath)) {
		t.Fatalf("WAL file data does not match")
	}

	expMetaPath := filepath.Join(currGenDir, "snap-1234", metaFileName)
	if !fileExists(expMetaPath) {
		t.Fatalf("meta file does not exist at %s", expMetaPath)
	}
}

func mustNewStoreForSinkTest(t *testing.T) *Store {
	tmpDir := t.TempDir()
	str, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return str
}

func mustCreateDir(path string) {
	if err := os.MkdirAll(path, 0755); err != nil {
		panic(err)
	}
}

func mustReadFile(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return b
}

func makeTestConfiguration(i, a string) raft.Configuration {
	return raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(i),
				Address: raft.ServerAddress(a),
			},
		},
	}
}

func makeMeta(id string, index, term, cfgIndex uint64) *Meta {
	return &Meta{
		SnapshotMeta: raft.SnapshotMeta{
			ID:                 id,
			Index:              index,
			Term:               term,
			Configuration:      makeTestConfiguration("1", "localhost:1"),
			ConfigurationIndex: cfgIndex,
			Version:            1,
		},
	}
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
