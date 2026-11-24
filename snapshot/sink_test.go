package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/encoding"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/rsum"
	"github.com/rqlite/rqlite/v10/snapshot/sidecar"
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
	if !fsutil.FilesIdentical("testdata/db-and-wals/full2.db", sink.DBFile()) {
		t.Fatalf("expected file %s to be identical to source", sink.DBFile())
	}

	// CRC file should exist and match the installed DB file.
	mustVerifyCRC32File(t, sink.DBFile())
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

	// DB and WAL files should be present with valid CRC sidecars.
	mustVerifyCRC32File(t, sink.DBFile())
	mustVerifyCRC32File(t, filepath.Join(dir, "data-00000000.wal"))
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

	// DB and WAL files should be present with valid CRC sidecars.
	mustVerifyCRC32File(t, sink.DBFile())
	mustVerifyCRC32File(t, filepath.Join(dir, "data-00000000.wal"))
	mustVerifyCRC32File(t, filepath.Join(dir, "data-00000001.wal"))
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

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if err := sink.IngestWALDir(NewStagingDir(walDir)); err != nil {
		t.Fatalf("unexpected error ingesting WAL directory: %s", err.Error())
	}

	// Writes should fail since no data is written to an incremental snapshot,
	// as should ingesting a second time.
	if _, err := sink.Write([]byte("extra data")); err == nil {
		t.Fatalf("expected error writing data to incremental snapshot, got nil")
	}
	if err := sink.IngestWALDir(NewStagingDir(walDir)); err == nil {
		t.Fatalf("expected error ingesting WAL directory twice, got nil")
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Installed WAL file should be byte-for-byte identical to source.
	walFile := filepath.Join(snapDir, meta.ID, walName)
	if !fsutil.FilesIdentical(srcPath, walFile) {
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

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-2", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if err := sink.IngestWALDir(NewStagingDir(walDir)); err != nil {
		t.Fatalf("unexpected error ingesting WAL directory: %s", err.Error())
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Each installed WAL file should be byte-for-byte identical to its source.
	for i, src := range srcPaths {
		walFile := filepath.Join(snapDir, meta.ID, walNames[i])
		if !fsutil.FilesIdentical(src, walFile) {
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

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-3", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if err := sink.IngestWALDir(NewStagingDir(walDir)); err != nil {
		t.Fatalf("unexpected error ingesting WAL directory: %s", err.Error())
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing sink: %s", err.Error())
	}

	// Each installed WAL file should be byte-for-byte identical to its source.
	for i, src := range srcPaths {
		walFile := filepath.Join(snapDir, meta.ID, walNames[i])
		if !fsutil.FilesIdentical(src, walFile) {
			t.Fatalf("expected WAL file %s to be identical to source %s", walFile, src)
		}
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

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-incremental-file-missing-crc", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil, nil)
	sink.fatalFn = nil // disable hard exit so error propagates to caller
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	if err := sink.IngestWALDir(NewStagingDir(walDir)); err != nil {
		t.Fatalf("unexpected error ingesting WAL directory: %s", err.Error())
	}

	// Close should fail because the .crc32 file is missing.
	if err := sink.Close(); err == nil {
		t.Fatalf("expected error closing sink with missing CRC file, got nil")
	}
}

// Test_Sink_WriteCannotNameDirectory tests that a snapshot stream cannot direct
// the sink at a directory on the local filesystem. The stream written is one
// which an earlier version of the header format used to do exactly that.
func Test_Sink_WriteCannotNameDirectory(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal-dir")
	if err := os.Mkdir(walDir, 0755); err != nil {
		t.Fatalf("unexpected error creating WAL dir: %s", err.Error())
	}
	walPath := filepath.Join(walDir, "00000000000000000001.wal")
	mustCopyFile(t, "testdata/db-and-wals/wal-01", walPath)
	mustWriteCRC32File(t, walPath)

	// SnapshotHeader{format_version: 1, <field 3>: {<field 1>: walDir}}
	inner := append([]byte{0x0a, byte(len(walDir))}, walDir...)
	hdrBytes := append([]byte{0x08, 0x01, 0x1a, byte(len(inner))}, inner...)
	if len(walDir) > 0x7f || len(inner) > 0x7f {
		t.Fatalf("WAL dir path too long for single-byte length encoding: %s", walDir)
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	snapDir := t.TempDir()
	meta := makeRaftMeta("test-write-cannot-name-dir", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil, nil)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}
	if _, err := sink.Write(frameBuf.Bytes()); err == nil {
		t.Fatalf("expected error writing header naming a directory, got nil")
	}

	// Raft cancels a sink it fails to write to. It may also close it, which must
	// neither succeed nor exit the process (fatalFn is at its default here).
	if err := sink.Close(); err == nil {
		t.Fatalf("expected error closing sink, got nil")
	}
	if err := sink.Cancel(); err != nil {
		t.Fatalf("unexpected error cancelling sink: %s", err.Error())
	}
	if !fsutil.FileExists(walPath) {
		t.Fatalf("expected WAL file %s to be untouched", walPath)
	}
	if fsutil.DirExists(filepath.Join(snapDir, meta.ID)) {
		t.Fatalf("expected no snapshot to be installed")
	}
}

func Test_Sink_CloseCh(t *testing.T) {
	header, err := NewSnapshotHeader("testdata/db-and-wals/full2.db")
	if err != nil {
		t.Fatalf("unexpected error creating header: %s", err.Error())
	}
	hdrBytes, err := marshalSnapshotHeader(header)
	if err != nil {
		t.Fatalf("unexpected error marshaling header: %s", err.Error())
	}

	var frameBuf bytes.Buffer
	lenBuf := make([]byte, HeaderSizeLen)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(hdrBytes)))
	frameBuf.Write(lenBuf)
	frameBuf.Write(hdrBytes)

	closeCh := make(chan struct{}, 1)
	snapDir := t.TempDir()
	meta := makeRaftMeta("test-close-ch", 100, 1, 1)
	sink := NewSink(snapDir, meta, nil, closeCh)
	if err := sink.Open(); err != nil {
		t.Fatalf("unexpected error opening sink: %s", err.Error())
	}

	// Write framed header + DB payload.
	if _, err := sink.Write(frameBuf.Bytes()); err != nil {
		t.Fatalf("unexpected error writing header to sink: %s", err.Error())
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

	select {
	case <-closeCh:
	case <-time.After(time.Second):
		t.Fatal("expected signal on closeCh after successful Close")
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
	if err := sidecar.WriteFile(path+crcSuffix, sum); err != nil {
		t.Fatalf("failed to write CRC32 sum file for %s: %v", path, err)
	}
}

// mustVerifyCRC32File checks that a .crc32 file exists alongside the data file
// at path and that its checksum matches the data file's actual CRC32.
func mustVerifyCRC32File(t *testing.T, path string) {
	t.Helper()
	ok, err := sidecar.CompareFile(path, path+crcSuffix)
	if err != nil {
		t.Fatalf("failed to compare CRC32 for %s: %v", path, err)
	}
	if !ok {
		t.Fatalf("CRC32 mismatch for %s", path)
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
