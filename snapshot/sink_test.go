package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/command/encoding"
	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/rsum"
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
	srcPath := "testdata/db-and-wals/wal-01"
	walName := "00000000000000000001.wal"

	// Create a directory containing the WAL file (mimics what the store does).
	walDir := filepath.Join(t.TempDir(), "wal-dir")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("unexpected error creating WAL dir: %s", err.Error())
	}
	mustCopyFile(t, srcPath, filepath.Join(walDir, walName))
	mustWriteCRC32File(t, filepath.Join(walDir, walName))

	hdr, err := NewIncrementalFileSnapshotHeader(walDir)
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
	walFile := filepath.Join(snapDir, meta.ID, walName)
	if !filesIdentical(srcPath, walFile) {
		t.Fatalf("expected WAL file %s to be identical to source", walFile)
	}
}

func Test_IncrementalFileSink_TwoFiles(t *testing.T) {
	srcPaths := []string{"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01"}
	walNames := []string{"00000000000000000001.wal", "00000000000000000002.wal"}

	// Create a directory containing the WAL files.
	walDir := filepath.Join(t.TempDir(), "wal-dir")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("unexpected error creating WAL dir: %s", err.Error())
	}
	for i, src := range srcPaths {
		mustCopyFile(t, src, filepath.Join(walDir, walNames[i]))
		mustWriteCRC32File(t, filepath.Join(walDir, walNames[i]))
	}

	hdr, err := NewIncrementalFileSnapshotHeader(walDir)
	if err != nil {
		t.Fatalf("unexpected error creating header: %s", err.Error())
	}
	hdrBytes, err := marshalSnapshotHeader(hdr)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-2", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Each installed WAL file should be byte-for-byte identical to its source.
	for i, src := range srcPaths {
		walFile := filepath.Join(snapDir, meta.ID, walNames[i])
		if !filesIdentical(src, walFile) {
			t.Fatalf("expected WAL file %s to be identical to source %s", walFile, src)
		}
	}
}

func Test_IncrementalFileSink_ThreeFiles(t *testing.T) {
	srcPaths := []string{"testdata/db-and-wals/wal-00", "testdata/db-and-wals/wal-01", "testdata/db-and-wals/wal-02"}
	walNames := []string{"00000000000000000001.wal", "00000000000000000002.wal", "00000000000000000003.wal"}

	// Create a directory containing the WAL files.
	walDir := filepath.Join(t.TempDir(), "wal-dir")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("unexpected error creating WAL dir: %s", err.Error())
	}
	for i, src := range srcPaths {
		mustCopyFile(t, src, filepath.Join(walDir, walNames[i]))
		mustWriteCRC32File(t, filepath.Join(walDir, walNames[i]))
	}

	hdr, err := NewIncrementalFileSnapshotHeader(walDir)
	if err != nil {
		t.Fatalf("unexpected error creating header: %s", err.Error())
	}
	hdrBytes, err := marshalSnapshotHeader(hdr)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-3", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Each installed WAL file should be byte-for-byte identical to its source.
	for i, src := range srcPaths {
		walFile := filepath.Join(snapDir, meta.ID, walNames[i])
		if !filesIdentical(src, walFile) {
			t.Fatalf("expected WAL file %s to be identical to source %s", walFile, src)
		}
	}
}

func Test_NoopSink(t *testing.T) {
	hdr := NewNoopSnapshotHeader()
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
	meta := makeRaftMeta("test-noop", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	// Write the framed header to the sink.
	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
	}

	// Additional writes should fail since no data follows a NoopSnapshot.
	if _, err := sink.Write([]byte("extra data")); err == nil {
		t.Fatalf("expected error writing extra data after noop header, got nil")
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// The data.noop sentinel file should exist in the snapshot directory.
	noopFile := filepath.Join(snapDir, meta.ID, noopfileName)
	if !fileExists(noopFile) {
		t.Fatalf("expected data.noop sentinel file at %s, but it does not exist", noopFile)
	}

	// meta.json should also exist.
	metaFile := filepath.Join(snapDir, meta.ID, metaFileName)
	if !fileExists(metaFile) {
		t.Fatalf("expected meta.json at %s, but it does not exist", metaFile)
	}
}

func Test_NoopSink_FullNeeded(t *testing.T) {
	hdr := NewNoopSnapshotHeader()
	hdrBytes, err := marshalSnapshotHeader(hdr)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	store, err := NewStore(snapDir)
	if err != nil {
		t.Fatalf("unexpected error creating store: %s", err.Error())
	}

	meta := makeRaftMeta("test-noop-fn", 100, 1, 1)
	sink := NewSink(snapDir, meta, store)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	// Store is empty, so FullNeeded should be true — noop should be rejected.
	if _, err := sink.Write(frameBuf.Bytes()); err == nil {
		t.Fatalf("expected error writing noop to empty store, got nil")
	}
}

func Test_IncrementalFileSink_MissingCRC(t *testing.T) {
	srcPath := "testdata/db-and-wals/wal-01"
	walName := "00000000000000000001.wal"

	// Create a directory containing the WAL file but NO .crc32 file.
	walDir := filepath.Join(t.TempDir(), "wal-dir")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("unexpected error creating WAL dir: %s", err.Error())
	}
	mustCopyFile(t, srcPath, filepath.Join(walDir, walName))

	hdr, err := NewIncrementalFileSnapshotHeader(walDir)
	if err != nil {
		t.Fatalf("unexpected error creating header: %s", err.Error())
	}
	hdrBytes, err := marshalSnapshotHeader(hdr)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-missing-crc", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
	}

	// Close should fail because the .crc32 file is missing.
	if err := sink.Close(); err == nil {
		t.Fatalf("expected error closing sink with missing CRC file, got nil")
	}
}

func Test_IncrementalFileSink_BadCRC(t *testing.T) {
	srcPath := "testdata/db-and-wals/wal-01"
	walName := "00000000000000000001.wal"

	// Create a directory containing the WAL file and a .crc32 file with the wrong value.
	walDir := filepath.Join(t.TempDir(), "wal-dir")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("unexpected error creating WAL dir: %s", err.Error())
	}
	walPath := filepath.Join(walDir, walName)
	mustCopyFile(t, srcPath, walPath)

	// Write a bogus CRC32 value.
	if err := rsum.WriteCRC32SumFile(walPath+crcSuffix, 0xdeadbeef); err != nil {
		t.Fatalf("unexpected error writing bogus CRC file: %s", err.Error())
	}

	hdr, err := NewIncrementalFileSnapshotHeader(walDir)
	if err != nil {
		t.Fatalf("unexpected error creating header: %s", err.Error())
	}
	hdrBytes, err := marshalSnapshotHeader(hdr)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-bad-crc", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
	}

	// Close should fail because the CRC32 checksum doesn't match.
	if err := sink.Close(); err == nil {
		t.Fatalf("expected error closing sink with bad CRC, got nil")
	}
}

// mustWriteCRC32File computes the CRC32 checksum for the file at path and writes
// the corresponding .crc32 file alongside it.
func mustWriteCRC32File(t *testing.T, path string) {
	t.Helper()
	sum, err := rsum.CRC32(path)
	if err != nil {
		t.Fatalf("failed to compute CRC32 for %s: %v", path, err)
	}
	if err := rsum.WriteCRC32SumFile(path+crcSuffix, sum); err != nil {
		t.Fatalf("failed to write CRC32 sum file for %s: %v", path, err)
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
