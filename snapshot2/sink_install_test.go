package snapshot2

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

func Test_NewInstallSink(t *testing.T) {
	install, err := proto.NewSnapshotInstall("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}

	sink := NewInstallSink(t.TempDir(), install)
	if sink == nil {
		t.Fatalf("expected non-nil Sink")
	}
}

func Test_InstallSink_SingleDBFile(t *testing.T) {
	install, err := proto.NewSnapshotInstall("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewInstallSink(dir, install)
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
	targetPath := filepath.Join(dir, "data.db")
	if !fileExists(targetPath) {
		t.Fatalf("expected file %s to exist, but does not", targetPath)
	}

	if !filesIdentical("testdata/db-and-wals/full2.db", targetPath) {
		t.Fatalf("expected file %s to be identical to source", targetPath)
	}
}

func Test_InstallSink_SingleDBFile_SingleWALFile(t *testing.T) {
	install, err := proto.NewSnapshotInstall(
		"testdata/db-and-wals/full2.db",
		"testdata/db-and-wals/wal-00")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewInstallSink(dir, install)
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

	targetPath := filepath.Join(dir, "data.db")
	if !filesIdentical("testdata/db-and-wals/full2.db", targetPath) {
		t.Fatalf("expected file %s to be identical to source", targetPath)
	}

	targetWALPath := filepath.Join(dir, "data-00000000.wal")
	if !filesIdentical("testdata/db-and-wals/wal-00", targetWALPath) {
		t.Fatalf("expected file %s to be identical to source", targetWALPath)
	}
}

func Test_InstallSink_SingleDBFile_MultiWALFile(t *testing.T) {
	install, err := proto.NewSnapshotInstall(
		"testdata/db-and-wals/full2.db",
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewInstallSink(dir, install)
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

	targetPath := filepath.Join(dir, "data.db")
	if !filesIdentical("testdata/db-and-wals/full2.db", targetPath) {
		t.Fatalf("expected file %s to be identical to source", targetPath)
	}

	targetWALPath0 := filepath.Join(dir, "data-00000000.wal")
	if !filesIdentical("testdata/db-and-wals/wal-00", targetWALPath0) {
		t.Fatalf("expected file %s to be identical to source", targetWALPath0)
	}
	targetWALPath1 := filepath.Join(dir, "data-00000001.wal")
	if !filesIdentical("testdata/db-and-wals/wal-01", targetWALPath1) {
		t.Fatalf("expected file %s to be identical to source", targetWALPath1)
	}
}
