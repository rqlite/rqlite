package backup

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auto/file"
)

func Test_Uploader_FileStorage_Timestamped(t *testing.T) {
	ResetStats()

	dir := t.TempDir()
	name := "backup.sqlite"
	storageClient, err := file.NewClient(dir, name, &file.Options{Timestamp: true})
	if err != nil {
		t.Fatalf("failed to create file storage client: %s", err.Error())
	}
	dp := &mockDataProvider{data: "test backup data"}
	interval := 10 * time.Millisecond
	uploader := NewUploader(storageClient, dp, interval)

	// Track last index values to force multiple uploads
	lastIndexValue := atomic.Uint64{}
	lastIndexValue.Store(1)
	dp.lastIndexFn = func() (uint64, error) {
		return lastIndexValue.Load(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	uploader.Start(ctx, nil)
	defer cancel()

	// Wait for first upload
	testPoll(t, func() bool {
		md, err := storageClient.CurrentMetadata(context.Background())
		return err == nil && md != nil
	}, 10*time.Millisecond, time.Second)

	// Add a small delay to ensure different timestamp
	time.Sleep(1100 * time.Millisecond)

	// Increment last index to trigger another upload
	lastIndexValue.Store(2)

	// Wait for second upload
	testPoll(t, func() bool {
		files, err := os.ReadDir(dir)
		if err != nil {
			return false
		}
		timestampedFiles := 0
		for _, file := range files {
			if strings.HasSuffix(file.Name(), "_backup.sqlite") {
				timestampedFiles++
			}
		}
		return timestampedFiles >= 2
	}, 50*time.Millisecond, 5*time.Second)

	// Verify we have at least 2 timestamped files
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read directory: %s", err.Error())
	}
	timestampedFiles := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), "_backup.sqlite") {
			timestampedFiles++
		}
	}
	if timestampedFiles < 2 {
		t.Fatalf("expected at least 2 timestamped files, got %d", timestampedFiles)
	}

	// Verify metadata points to the latest file
	md, err := storageClient.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("failed to get current metadata: %s", err.Error())
	}
	if md == nil {
		t.Fatal("metadata is nil")
	}

	// Verify data in latest file is correct
	dataPath := storageClient.LatestFilePath(context.Background())
	if dataPath == "" {
		t.Fatal("latest file path is empty")
	}
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("failed to read data file: %s", err.Error())
	}
	if string(data) != "test backup data" {
		t.Fatalf("data mismatch: got %q want %q", string(data), "test backup data")
	}
}

func Test_Uploader_FileStorage(t *testing.T) {
	ResetStats()

	dir := t.TempDir()
	name := "data"
	storageClient, err := file.NewClient(dir, name, nil)
	if err != nil {
		t.Fatalf("failed to create file storage client: %s", err.Error())
	}
	dp := &mockDataProvider{data: "my upload data"}
	interval := 10 * time.Millisecond
	uploader := NewUploader(storageClient, dp, interval)

	uploaded := false
	enabledFn := func() bool {
		if !uploaded {
			uploaded = true
			return true
		}
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	uploader.Start(ctx, enabledFn)
	defer cancel()

	testPoll(t, func() bool {
		md, err := storageClient.CurrentMetadata(context.Background())
		return err == nil && md != nil
	}, 10*time.Millisecond, time.Second)

	md, err := storageClient.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("failed to get current metadata: %s", err.Error())
	}
	if md == nil {
		t.Fatal("metadata is nil")
	}
	if md.ID != "1" {
		t.Fatalf("expected metadata ID to be 1, got %s", md.ID)
	}

	dataPath := storageClient.LatestFilePath(context.Background())
	if dataPath == "" {
		t.Fatal("latest file path is empty")
	}
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("failed to read data file: %s", err.Error())
	}
	if string(data) != "my upload data" {
		t.Fatalf("data mismatch: got %q want %q", string(data), "my upload data")
	}
}
