package file

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if md.Name != c.LatestFilePath(context.Background()) {
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

func Test_Upload_Timestamp_True(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "backup.sqlite", &Options{Timestamp: true})

	data1 := []byte("data1")
	data2 := []byte("data2")
	id1 := "v1"
	id2 := "v2"

	// Upload first file
	if err := c.Upload(context.Background(), bytes.NewReader(data1), id1); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	// Sleep to ensure different timestamp
	time.Sleep(1100 * time.Millisecond) // Just over 1 second to ensure different timestamp

	// Upload second file
	if err := c.Upload(context.Background(), bytes.NewReader(data2), id2); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	// Check that two distinct files exist
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}

	dataFiles := []string{}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), "_backup.sqlite") {
			dataFiles = append(dataFiles, entry.Name())
		}
	}

	if len(dataFiles) != 2 {
		t.Fatalf("expected 2 data files, got %d: %v", len(dataFiles), dataFiles)
	}

	// Check metadata points to the latest file
	md, err := c.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("CurrentMetadata error: %v", err)
	}
	if md.ID != id2 {
		t.Fatalf("id mismatch: got %q want %q", md.ID, id2)
	}

	// Verify the latest file path contains the second data
	latestPath := c.LatestFilePath(context.Background())
	if latestPath == "" {
		t.Fatal("latest file path is empty")
	}
	gotData, err := os.ReadFile(latestPath)
	if err != nil {
		t.Fatalf("read latest file: %v", err)
	}
	if !bytes.Equal(gotData, data2) {
		t.Fatalf("latest file data mismatch: got %q want %q", string(gotData), string(data2))
	}
}

func Test_Upload_Timestamp_False(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "backup.sqlite", &Options{Timestamp: false})

	data1 := []byte("data1")
	data2 := []byte("data2")
	id1 := "v1"
	id2 := "v2"

	// Upload first file
	if err := c.Upload(context.Background(), bytes.NewReader(data1), id1); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	// Upload second file (should overwrite)
	if err := c.Upload(context.Background(), bytes.NewReader(data2), id2); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	// Check that only one data file exists
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}

	dataFiles := []string{}
	for _, entry := range entries {
		if entry.Name() == "backup.sqlite" {
			dataFiles = append(dataFiles, entry.Name())
		}
	}

	if len(dataFiles) != 1 {
		t.Fatalf("expected 1 data file, got %d: %v", len(dataFiles), dataFiles)
	}

	// Verify the file contains the second data (overwritten)
	gotData, err := os.ReadFile(filepath.Join(dir, "backup.sqlite"))
	if err != nil {
		t.Fatalf("read data file: %v", err)
	}
	if !bytes.Equal(gotData, data2) {
		t.Fatalf("data mismatch: got %q want %q", string(gotData), string(data2))
	}

	// Check metadata points to the overwritten file
	md, err := c.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("CurrentMetadata error: %v", err)
	}
	if md.ID != id2 {
		t.Fatalf("id mismatch: got %q want %q", md.ID, id2)
	}
}

func Test_NewClient_PathValidation(t *testing.T) {
	dir := t.TempDir()

	// Test valid file names
	validFiles := []string{"backup.sqlite", "backup.zip", "data.bin"}
	for _, file := range validFiles {
		_, err := NewClient(dir, file, nil)
		if err != nil {
			t.Fatalf("expected valid file %q to succeed, got error: %v", file, err)
		}
	}

	// Test invalid file names (path traversal attempts)
	invalidFiles := []string{"../backup.sqlite", "/abs/path.sqlite", "dir/../backup.sqlite", "dir/backup.sqlite"}
	for _, file := range invalidFiles {
		_, err := NewClient(dir, file, nil)
		if err == nil {
			t.Fatalf("expected invalid file %q to fail, but it succeeded", file)
		}
		if !strings.Contains(err.Error(), "invalid file parameter") {
			t.Fatalf("expected path validation error for %q, got: %v", file, err)
		}
	}
}
