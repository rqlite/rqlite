package store

import (
	"bytes"
	"os"
	"testing"

	sql "github.com/rqlite/rqlite/v8/db"
)

func Test_SnapshotSource(t *testing.T) {
	path := mustCreateTempFile()
	defer os.Remove(path)
	db, err := sql.OpenSwappable(path, true, true)
	if err != nil {
		t.Fatalf("failed to open swappable db: %s", err)
	}
	defer db.Close()

	ss := NewSnapshotSource(db)
	if ss == nil {
		t.Fatalf("failed to create snapshot source")
	}
	_, rc, err := ss.Open()
	if err != nil {
		t.Fatalf("failed to open snapshot: %s", err)
	}
	defer rc.Close()

	srcBuf := mustReadFile(path)
	dstBuf := mustReadAll(rc)
	if !bytes.Equal(srcBuf, dstBuf) {
		t.Fatalf("snapshot source data does not match original data")
	}
}
