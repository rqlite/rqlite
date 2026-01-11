package snapshot2

import (
	"os"
	"testing"

	"github.com/rqlite/rqlite/v9/snapshot2/proto"
)

func test_SnapshotSinkDBTests[M any](t *testing.T, name string, sinkerFn func(dir string, m M) sinker, srcFile string, manifestFn func() M) {
	t.Run(name+"_Success", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		data, err := os.ReadFile(srcFile)
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
	})

	t.Run(name+"_NoCRC", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		// Zero out CRC32
		switch v := any(m).(type) {
		case *proto.SnapshotDBFile:
			v.Crc32 = 0
		case *proto.SnapshotWALFile:
			v.Crc32 = 0
		default:
			t.Fatalf("unsupported manifest type")
		}

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		data, err := os.ReadFile(srcFile)
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
	})

	t.Run(name+"_InvalidSourceFile", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		_, err := sink.Write([]byte("aklsjskdj"))
		if err != nil {
			t.Fatalf("failed to write to sink: %s", err)
		}

		if err := sink.Close(); err == nil {
			t.Fatalf("expected error when closing sink with invalid data")
		}
	})

	t.Run(name+"_FileShortWrite", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		data, err := os.ReadFile(srcFile)
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
	})

	t.Run(name+"_FileLongWrite", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		data, err := os.ReadFile(srcFile)
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
	})

	t.Run(name+"_FileSizeFail", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		// Corrupt the size
		switch v := any(m).(type) {
		case *proto.SnapshotDBFile:
			v.SizeBytes++
		case *proto.SnapshotWALFile:
			v.SizeBytes++
		default:
			t.Fatalf("unsupported manifest type")
		}

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		data, err := os.ReadFile(srcFile)
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
	})

	t.Run(name+"_CRCFail", func(t *testing.T) {
		dir := t.TempDir()
		m := manifestFn()

		// Corrupt the checksum
		switch v := any(m).(type) {
		case *proto.SnapshotDBFile:
			v.Crc32++
		case *proto.SnapshotWALFile:
			v.Crc32++
		default:
			t.Fatalf("unsupported manifest type")
		}

		sink := sinkerFn(dir, m)
		if sink == nil {
			t.Fatalf("expected non-nil sink")
		}

		if err := sink.Open(); err != nil {
			t.Fatalf("failed to open sink: %s", err)
		}

		data, err := os.ReadFile(srcFile)
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
			t.Fatalf("expected error when closing sink with bad CRC")
		}
	})
}

func Test_DB_WAL_Sinks(t *testing.T) {
	test_SnapshotSinkDBTests(
		t,
		"DBSink",
		func(dir string, m *proto.SnapshotDBFile) sinker {
			return NewDBSink(dir, m)
		}, "testdata/test.db", func() *proto.SnapshotDBFile {
			return mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", true)
		})

	test_SnapshotSinkDBTests(
		t,
		"WALSink",
		func(dir string, m *proto.SnapshotWALFile) sinker {
			return NewWALSink(dir, m)
		}, "testdata/wal", func() *proto.SnapshotWALFile {
			return mustCreateSnapshotWALFileFromFile(t, "testdata/wal", true)
		})
}

func Test_NewDBSink(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotDBFileFromFile(t, "testdata/test.db", false)
	sink := NewDBSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
	}
}

func Test_NewWALSink(t *testing.T) {
	dir := t.TempDir()

	m := mustCreateSnapshotWALFileFromFile(t, "testdata/wal", false)
	sink := NewWALSink(dir, m)
	if sink == nil {
		t.Fatalf("expected non-nil sink")
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

func mustCreateSnapshotWALFileFromFile(t *testing.T, path string, crc32 bool) *proto.SnapshotWALFile {
	t.Helper()
	m, err := proto.NewSnapshotWALFileFromFile(path, crc32)
	if err != nil {
		t.Fatalf("failed to create snapshot WAL file from %s: %s", path, err)
	}
	return m
}
