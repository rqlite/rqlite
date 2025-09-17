package file

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func Test_NewClient(t *testing.T) {
	dir := t.TempDir()
	c, err := NewClient(dir, "data.bin", &Options{})
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	if c == nil {
		t.Fatal("client is nil")
	}
}

func Test_CurrentID_NotExist(t *testing.T) {
	c, _ := NewClient(t.TempDir(), "x", nil)

	id, err := c.CurrentID(context.Background())
	if err != nil {
		t.Fatalf("CurrentID error: %v", err)
	}
	if id != "" {
		t.Fatalf("id = %q, want empty", id)
	}
}

func Test_Upload_Success(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "data.bin", nil)

	data := []byte("hello-world")
	id := "v1"

	if err := c.Upload(context.Background(), bytes.NewReader(data), id); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	// Final data file exists with content.
	gotData, err := os.ReadFile(filepath.Join(dir, "data.bin"))
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if !bytes.Equal(gotData, data) {
		t.Fatalf("data mismatch: got %q want %q", string(gotData), string(data))
	}

	md, err := c.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("CurrentMetadata error: %v", err)
	}
	if md == nil {
		t.Fatal("metadata is nil")
	}
	if md.ID != id {
		t.Fatalf("id mismatch: got %q want %q", md.ID, id)
	}
	if md.File != c.LatestFilePath(context.Background()) {
		t.Fatalf("file mismatch")
	}
	if md.Timestamp == 0 {
		t.Fatalf("timestamp is zero")
	}
}

func Test_Upload_RemovesExistingID(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "data.bin", nil)

	if err := c.Upload(context.Background(), bytes.NewReader([]byte("data1")), "old"); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	if err := c.Upload(context.Background(), bytes.NewReader([]byte("data2")), "new"); err != nil {
		t.Fatalf("Upload error: %v", err)
	}
	md, err := c.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("CurrentMetadata error: %v", err)
	}
	if md.ID != "new" {
		t.Fatalf("id mismatch: got %q want %q", md.ID, "new")
	}
}
