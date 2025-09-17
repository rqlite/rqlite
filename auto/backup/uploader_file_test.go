package backup

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v9/auto/file"
)

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
