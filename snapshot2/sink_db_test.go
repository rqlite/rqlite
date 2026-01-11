package snapshot2

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

func Test_NewDBSink(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", false)
	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}
}

func Test_DBSink_Success(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", true)
	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(data)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}

	// Verify the written file exists.
	writtenPath := filepath.Join(dir, "db.sqlite3")
	if !fileExists(writtenPath) {
		t.Fatalf("expected written file to exist at %s", writtenPath)
	}
}

func Test_DBSink_Success_NoCRC(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", false)
	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(data)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close sink: %s", err)
	}

	// Verify the written file exists.
	writtenPath := filepath.Join(dir, "db.sqlite3")
	if !fileExists(writtenPath) {
		t.Fatalf("expected written file to exist at %s", writtenPath)
	}
}

func Test_DBSink_FileNotValidSQLite(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/invalid.db", true)

	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/invalid.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(data)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}

	if err := sink.Close(); err == nil {
		t.Fatalf("expected error when closing sink with invalid SQLite file")
	}
}

func Test_DBSink_FileShortWrite(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", true)

	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(data[:len(data)-1]) // Write less data than expected
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data)-1 {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data)-1, n)
	}

	if err := sink.Close(); err == nil {
		t.Fatalf("expected error when closing sink with short write")
	}
}

func Test_DBSink_FileLongWrite(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", true)

	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(append(data, 0)) // Write more data than expected
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data)+1 {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data)+1, n)
	}

	if err := sink.Close(); err == nil {
		t.Fatalf("expected error when closing sink with long write")
	}
}

func Test_DBSink_FileSizeFail(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", true)
	m.SizeBytes++ // Corrupt the size

	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(data)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}

	if err := sink.Close(); err == nil {
		t.Fatalf("expected error when closing sink with bad size")
	}
}

func Test_DBSink_CRCFail(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", true)
	m.Crc32++ // Corrupt the checksum

	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}

	if err := sink.Open(); err != nil {
		t.Fatalf("failed to open sink: %s", err)
	}

	data, err := os.ReadFile("testdata/test.db")
	if err != nil {
		t.Fatalf("failed to read test db file: %s", err)
	}

	n, err := sink.Write(data)
	if err != nil {
		t.Fatalf("failed to write to sink: %s", err)
	}

	if n != len(data) {
		t.Fatalf("expected to write %d bytes, wrote %d", len(data), n)
	}

	if err := sink.Close(); err == nil {
		t.Fatalf("expected error when closing sink with corrupted CRC")
	}
}

func mustCreateSnapshotDBFileFromFile(t *testing.T, path string, crc32 bool) *proto.SnapshotDBFile {
	t.Helper()
	m, err := proto.NewSnapshotDBFileFromFile(path, crc32)
	if err != nil {
		t.Fatalf("failed to create snapshot DB file from %s: %s", path, err)
	}
	return m
}
