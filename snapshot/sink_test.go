package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v9/command/encoding"
	"github.com/rqlite/rqlite/v9/db"
)

func Test_NewFullSink(t *testing.T) {
	hdr, err := NewSnapshotHeader("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}

	sink := NewFullSink(t.TempDir(), hdr.GetFull())
	if sink == nil {
		t.Fatalf("expected non-nil Sink")
	}
}

func Test_FullSink_SingleDBFile(t *testing.T) {
	header, err := NewSnapshotHeader("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewFullSink(dir, header.GetFull())
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

	// Installed DB file should be byte-for-byte identical to source.
	if !filesIdentical("testdata/db-and-wals/full2.db", sink.DBFile()) {
		t.Fatalf("expected file %s to be identical to source", sink.DBFile())
	}
}

func Test_FullSink_SingleDBFile_SingleWALFile(t *testing.T) {
	header, err := NewSnapshotHeader(
		"testdata/db-and-wals/backup.db",
		"testdata/db-and-wals/wal-00")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewFullSink(dir, header.GetFull())
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	for _, filePath := range []string{"testdata/db-and-wals/backup.db", "testdata/db-and-wals/wal-00"} {
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

	// Check the database state inside the Store.
	dbPath := sink.DBFile()
	checkDB, err := db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dbPath, err)
	}
	defer checkDB.Close()
	rows, err := checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}
}

func Test_FullSink_SingleDBFile_MultiWALFile(t *testing.T) {
	header, err := NewSnapshotHeader(
		"testdata/db-and-wals/backup.db",
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}
	dir := t.TempDir()
	sink := NewFullSink(dir, header.GetFull())
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	for _, filePath := range []string{
		"testdata/db-and-wals/backup.db",
		"testdata/db-and-wals/wal-00",
		"testdata/db-and-wals/wal-01"} {
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

	// Check the database state inside the Store.
	dbPath := sink.DBFile()
	checkDB, err := db.Open(dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database at %s: %s", dbPath, err)
	}
	defer checkDB.Close()
	rows, err := checkDB.QueryStringStmt("SELECT COUNT(*) FROM foo")
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[2]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
	}
}

func Test_IncrementalSink(t *testing.T) {
	hdr, err := NewSnapshotHeader("", "testdata/db-and-wals/wal-00")
	if err != nil {
		t.Fatalf("unexpected error creating manifest: %s", err.Error())
	}

	sink := NewIncrementalSink(t.TempDir(), hdr.GetIncremental().WalHeader)
	if sink == nil {
		t.Fatalf("expected non-nil Sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	fd, err := os.Open("testdata/db-and-wals/wal-00")
	if err != nil {
		t.Fatalf("unexpected error opening source wal file: %s", err.Error())
	}
	defer fd.Close()

	if _, err := io.Copy(sink, fd); err != nil {
		t.Fatalf("unexpected error copying data to sink: %s", err.Error())
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Installed WAL file should be byte-for-byte identical to source.
	if !filesIdentical("testdata/db-and-wals/wal-00", sink.WALFile()) {
		t.Fatalf("expected file %s to be identical to source", sink.WALFile())
	}
}

func Test_IncrementalFileSink(t *testing.T) {
	tempDir := t.TempDir()
	srcPath := "testdata/db-and-wals/wal-01"
	tmpSrcPath := filepath.Join(tempDir, "wal-00") // Because the file will be moved.
	mustCopyFile(t, srcPath, tmpSrcPath)

	hdr, err := NewIncrementalFileSnapshotHeader(tmpSrcPath)
	if err != nil {
		t.Fatalf("unexpected error creating header: %s", err.Error())
	}
	hdrBytes, err := marshalSnapshotHeader(hdr)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	// Build the framed message: 4-byte length prefix + header bytes.
	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	// Write the framed header to the sink.
	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
	}

	// Additional writes should fail since no data follows an IncrementalFileSnapshot.
	if _, err := sink.Write([]byte("extra data")); err == nil {
		t.Fatalf("expected error writing extra data after incremental file header, got nil")
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Installed WAL file should be byte-for-byte identical to source.
	walFile := filepath.Join(snapDir, meta.ID, walfileName)
	if !filesIdentical(srcPath, walFile) {
		t.Fatalf("expected WAL file %s to be identical to source", walFile)
	}
}

func asJSON(v any) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}
