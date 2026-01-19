package snapshot2

import (
	"io"
	"os"
	"testing"

	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

func Test_NewFullSink(t *testing.T) {
	install, err := proto.NewSnapshotHeader("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}

	sink := NewFullSink(t.TempDir(), install)
	if sink == nil {
		t.Fatalf("expected non-nil Sink")
	}
}

func Test_FullSink_SingleDBFile(t *testing.T) {
	header, err := proto.NewSnapshotHeader("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewFullSink(dir, header)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	fd, err := os.Open("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error opening source db file: %s", err.Error())
	}
	defer fd.Close()

	if _, err := io.Copy(sink, fd); err != nil {
		t.Fatalf("unexpected error copying data to sink: %s", err.Error())
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Verify file exists in target dir.
	if !fileExists(sink.DBFile()) {
		t.Fatalf("expected file %s to exist, but does not", sink.DBFile())
	}

	if !filesIdentical("testdata/db-and-wals/full2.db", sink.DBFile()) {
		t.Fatalf("expected file %s to be identical to source", sink.DBFile())
	}
	if sink.NumWALFiles() != 0 {
		t.Fatalf("expected 0 WAL files, got %d", sink.NumWALFiles())
	}
}

func Test_FullSink_SingleDBFile_SingleWALFile(t *testing.T) {
	header, err := proto.NewSnapshotHeader(
		"testdata/db-and-wals/full2.db",
		"testdata/db-and-wals/wal-00")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewFullSink(dir, header)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	for _, filePath := range []string{"testdata/db-and-wals/full2.db", "testdata/db-and-wals/wal-00"} {
		fd, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("unexpected error opening source file %s: %s", filePath, err.Error())
		}

		if _, err := io.Copy(sink, fd); err != nil {
			t.Fatalf("unexpected error copying data to sink: %s", err.Error())
		}
		fd.Close()
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	if !filesIdentical("testdata/db-and-wals/full2.db", sink.DBFile()) {
		t.Fatalf("expected file %s to be identical to source", sink.DBFile())
	}

	if sink.NumWALFiles() != 1 {
		t.Fatalf("expected 1 WAL file, got %d", sink.NumWALFiles())
	}
	targetWALPath := sink.WALFiles()[0]
	if !filesIdentical("testdata/db-and-wals/wal-00", targetWALPath) {
		t.Fatalf("expected file %s to be identical to source", targetWALPath)
	}
}

func Test_FullSink_SingleDBFile_MultiWALFile(t *testing.T) {
	header, err := proto.NewSnapshotHeader(
		"testdata/db-and-wals/full2.db",
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewFullSink(dir, header)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	for _, filePath := range []string{"testdata/db-and-wals/full2.db", "testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01"} {
		fd, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("unexpected error opening source file %s: %s", filePath, err.Error())
		}

		if _, err := io.Copy(sink, fd); err != nil {
			t.Fatalf("unexpected error copying data to sink: %s", err.Error())
		}
		fd.Close()
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	if !filesIdentical("testdata/db-and-wals/full2.db", sink.DBFile()) {
		t.Fatalf("expected file %s to be identical to source", sink.DBFile())
	}

	if sink.NumWALFiles() != 2 {
		t.Fatalf("expected 2 WAL files, got %d", sink.NumWALFiles())
	}
	targetWALPath0 := sink.WALFiles()[0]
	if !filesIdentical("testdata/db-and-wals/wal-00", targetWALPath0) {
		t.Fatalf("expected file %s to be identical to source", targetWALPath0)
	}
	targetWALPath1 := sink.WALFiles()[1]
	if !filesIdentical("testdata/db-and-wals/wal-01", targetWALPath1) {
		t.Fatalf("expected file %s to be identical to source", targetWALPath1)
	}
}
