package file

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func TestUploadMetadataRenameFailurePreservesLastSuccessfulBackup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		timestamp bool
	}{
		{"stable filename", false},
		{"timestamped filename", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			client, err := NewClient(dir, "backup.sqlite", &Options{Timestamp: tc.timestamp})
			if err != nil {
				t.Fatal(err)
			}
			// Ensure two different timestamped names without waiting.
			times := []time.Time{
				time.Date(2026, 10, 25, 15, 0, 0, 0, time.UTC),
				time.Date(2026, 10, 25, 15, 0, 2, 0, time.UTC),
			}
			client.now = func() time.Time {
				latest := times[0]
				times = times[1:]
				return latest
			}
			old := []byte("last-committed-contents")
			if err := client.Upload(ctx, bytes.NewReader(old), "old"); err != nil {
				t.Fatal(err)
			}
			before, err := client.CurrentMetadata(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if before == nil || before.ID != "old" {
				t.Fatalf("initial metadata %+v", before)
			}
			expectedError := errors.New("injected metadata rename failure")
			client.renameFile = func(source, dest string) error {
				if dest == client.metaPath {
					return expectedError
				}
				return fsutil.Rename(source, dest)
			}
			if err := client.Upload(ctx, bytes.NewReader([]byte("new-candidate")), "new"); !errors.Is(err, expectedError) {
				t.Fatalf("Upload() error = %v, want injected failure", err)
			}
			after, err := client.CurrentMetadata(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if after == nil || after.ID != before.ID || after.Name != before.Name {
				t.Fatalf("metadata changed across failed upload: before=%+v, after=%+v", before, after)
			}
			got, err := os.ReadFile(before.Name)
			if err != nil {
				t.Fatalf("previously committed backup missing after failed upload: %v", err)
			}
			if !bytes.Equal(got, old) {
				t.Fatalf("previously committed backup overwritten: got %q want %q", got, old)
			}
		})
	}
}

func TestUploadFirstMetadataRenameFailureLeavesNoPublishedFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	client, err := NewClient(dir, "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	denied := errors.New("injected metadata rename failure")
	client.renameFile = func(src, dest string) error {
		if dest == client.metaPath {
			return denied
		}
		return fsutil.Rename(src, dest)
	}
	if err := client.Upload(ctx, bytes.NewReader([]byte("first")), "first"); !errors.Is(err, denied) {
		t.Fatalf("Upload error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "backup.sqlite")); !os.IsNotExist(err) {
		t.Fatalf("unexpected first backup after failed metadata publish: %v", err)
	}
	if md, err := client.CurrentMetadata(ctx); err != nil || md != nil {
		t.Fatalf("unexpected metadata: %+v, %v", md, err)
	}
}

func TestUploadPayloadRenameFailureKeepsPreviousBackup(t *testing.T) {
	ctx := context.Background()
	client, err := NewClient(t.TempDir(), "backup.sqlite", nil)
	if err != nil {
		t.Fatal(err)
	}
	old := []byte("last-committed")
	if err := client.Upload(ctx, bytes.NewReader(old), "old"); err != nil {
		t.Fatal(err)
	}
	denied := errors.New("injected payload rename failure")
	client.renameFile = func(src, dest string) error {
		if dest == filepath.Join(client.dir, client.name) {
			return denied
		}
		return fsutil.Rename(src, dest)
	}
	if err := client.Upload(ctx, bytes.NewReader([]byte("new")), "new"); !errors.Is(err, denied) {
		t.Fatalf("Upload error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(client.dir, client.name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, old) {
		t.Fatalf("prior data changed: %q", data)
	}
}
