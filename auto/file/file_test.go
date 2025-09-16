package file

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func Test_NewClientPaths(t *testing.T) {
	dir := t.TempDir()
	c, err := NewClient(dir, "data.bin", &Options{})
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	wantPath := filepath.Join(dir, "data.bin")
	wantIDPath := filepath.Join(dir, "METADATA")
	if c.path != wantPath {
		t.Fatalf("path mismatch: got %q want %q", c.path, wantPath)
	}
	if c.idPath != wantIDPath {
		t.Fatalf("idPath mismatch: got %q want %q", c.idPath, wantIDPath)
	}
}

func Test_String(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "data.bin", nil)
	got := c.String()
	want := fmt.Sprintf("file:%s", filepath.Join(dir, "data.bin"))
	if got != want {
		t.Fatalf("String() = %q want %q", got, want)
	}
}

func Test_CurrentID_NotExist(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "x", nil)

	id, ok, err := c.CurrentID(context.Background())
	if err != nil {
		t.Fatalf("CurrentID error: %v", err)
	}
	if ok {
		t.Fatalf("ok = true, want false for missing METADATA")
	}
	if id != "" {
		t.Fatalf("id = %q, want empty", id)
	}
}

func Test_CurrentID_ReadError(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "x", nil)

	// Make METADATA a directory so reads fail.
	if err := os.Mkdir(c.idPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	_, _, err := c.CurrentID(context.Background())
	if err == nil {
		t.Fatalf("expected error when METADATA is a directory")
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
	gotData, err := os.ReadFile(c.path)
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	if !bytes.Equal(gotData, data) {
		t.Fatalf("data mismatch: got %q want %q", string(gotData), string(data))
	}

	// METADATA file exists with id.
	gotID, err := os.ReadFile(c.idPath)
	if err != nil {
		t.Fatalf("read id: %v", err)
	}
	if string(gotID) != id {
		t.Fatalf("id mismatch: got %q want %q", string(gotID), id)
	}

	// .tmp files should not remain.
	if _, err := os.Stat(c.path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp data file should not exist")
	}
	if _, err := os.Stat(c.idPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp id file should not exist")
	}
}

func Test_Upload_RemovesExistingID(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "data.bin", nil)

	// Seed an existing METADATA file.
	if err := os.WriteFile(c.idPath, []byte("old"), 0o644); err != nil {
		t.Fatalf("seed id: %v", err)
	}

	if err := c.Upload(context.Background(), bytes.NewReader([]byte("data")), "new"); err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	gotID, err := os.ReadFile(c.idPath)
	if err != nil {
		t.Fatalf("read id: %v", err)
	}
	if string(gotID) != "new" {
		t.Fatalf("id mismatch: got %q want %q", string(gotID), "new")
	}
}

type errReader struct{}

func (e *errReader) Read(p []byte) (int, error) { return 0, fmt.Errorf("read-failed") }

func Test_Upload_CopyError_CleansTmpAndLeavesNoFinals(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "data.bin", nil)

	// Force io.Copy to fail.
	r := &errReader{}
	err := c.Upload(context.Background(), r, "id-ignored")
	if err == nil {
		t.Fatalf("expected upload error")
	}

	// No final files should exist.
	if _, statErr := os.Stat(c.path); !os.IsNotExist(statErr) {
		t.Fatalf("final data file should not exist")
	}
	if _, statErr := os.Stat(c.idPath); !os.IsNotExist(statErr) {
		t.Fatalf("final id file should not exist")
	}

	// .tmp files should be cleaned.
	if _, statErr := os.Stat(c.path + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("tmp data file should be removed")
	}
	if _, statErr := os.Stat(c.idPath + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("tmp id file should be removed")
	}
}

func Test_Upload_FailsIfIDPathRemoveError(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewClient(dir, "data.bin", nil)

	// Make METADATA a directory to force Remove error that is not IsNotExist.
	if err := os.Mkdir(c.idPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	err := c.Upload(context.Background(), bytes.NewReader([]byte("x")), "id")
	if err == nil {
		t.Fatalf("expected error due to failing to remove METADATA directory")
	}

	// Ensure no tmp or final artifacts left behind.
	if _, statErr := os.Stat(c.path); !os.IsNotExist(statErr) {
		t.Fatalf("final data file should not exist")
	}
	if _, statErr := os.Stat(c.path + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("tmp data file should not exist")
	}
	if _, statErr := os.Stat(c.idPath + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("tmp id file should not exist")
	}
}
