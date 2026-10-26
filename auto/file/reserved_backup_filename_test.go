package file

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewClientRejectsInternalBackupFilenames(t *testing.T) {
	for _, name := range []string{
		"METADATA.json",
		"METADATA.json.tmp",
		".touch",
		"metadata.JSON",
		"metadata.json.TMP",
		".TOUCH",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			c, err := NewClient(dir, name, nil)
			if err == nil {
				// Demonstrate the observed corruption without a live DB.
				if name == "METADATA.json" {
					data := []byte("synthetic-backup-data")
					if uploadErr := c.Upload(context.Background(), bytes.NewReader(data), "v1"); uploadErr == nil {
						raw, readErr := os.ReadFile(c.LatestFilePath(context.Background()))
						t.Logf("original upload returned success, stored bytes=%q read error=%v (expected backup=%q)", raw, readErr, data)
					}
				}
				if name == ".touch" {
					data := []byte("previous-committed")
					if uploadErr := c.Upload(context.Background(), bytes.NewReader(data), "old"); uploadErr == nil {
						_, reopenErr := NewClient(dir, name, nil)
						_, fileErr := os.Stat(filepath.Join(dir, name))
						t.Logf("original reopen returned %v, previous backup stat=%v", reopenErr, fileErr)
					}
				}
				t.Errorf("NewClient(%q) accepted a filename reserved for internal backup state", name)
			} else if !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("NewClient(%q) = %v, want explicit reserved filename error", name, err)
			}
			if _, err := os.Stat(filepath.Join(dir, ".touch")); !os.IsNotExist(err) {
				t.Errorf("rejected name should not create a writable probe file, got %v", err)
			}
		})
	}
}

func TestNewClientAllowsOrdinaryBackupFilenames(t *testing.T) {
	for _, name := range []string{"backup.sqlite", "user.db", "backup.zip"} {
		t.Run(name, func(t *testing.T) {
			c, err := NewClient(t.TempDir(), name, nil)
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("fixture-backup")
			if err := c.Upload(context.Background(), bytes.NewReader(data), "good"); err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(c.LatestFilePath(context.Background()))
			if err != nil || !bytes.Equal(actual, data) {
				t.Fatalf("backup %q differs: bytes=%q err=%v", name, actual, err)
			}
		})
	}
}
