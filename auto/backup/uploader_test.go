package backup

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Test_NewUploader(t *testing.T) {
	ResetStats()
	storageClient := &mockStorageClient{}
	dataProvider := &mockDataProvider{}
	interval := time.Second
	uploader := NewUploader(storageClient, dataProvider, interval, UploadNoCompress)
	if uploader.storageClient != storageClient {
		t.Errorf("expected storageClient to be %v, got %v", storageClient, uploader.storageClient)
	}
	if uploader.dataProvider != dataProvider {
		t.Errorf("expected dataProvider to be %v, got %v", dataProvider, uploader.dataProvider)
	}
	if uploader.interval != interval {
		t.Errorf("expected interval to be %v, got %v", interval, uploader.interval)
	}
}

func Test_UploaderSingleUpload(t *testing.T) {
	ResetStats()
	var uploadedData []byte
	var err error

	var wg sync.WaitGroup
	wg.Add(1)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			defer wg.Done()
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond, UploadNoCompress)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-done
	if exp, got := "my upload data", string(uploadedData); exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}

	time.Sleep(time.Second)
	if exp, got := int64(0), stats.Get(numUploadsSkipped).(*expvar.Int); exp < got.Value() {
		t.Errorf("expected numUploadsSkipped to be > %d, got %d", exp, got)
	}
}

// Test_UploaderSingleUpload_ID ensures that when the ID in the
// storage service is the same as the ID of the data being uploaded, the
// upload is skipped.
func Test_UploaderSingleUpload_ID(t *testing.T) {
	ResetStats()
	var wg sync.WaitGroup
	wg.Add(1)
	nCalled := 0
	sc := &mockStorageClient{
		currentIDFn: func(ctx context.Context) (string, error) {
			nCalled++
			if nCalled == 0 {
				defer wg.Done()
			}
			return "1234", nil
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	dp.lastIndexFn = func() (uint64, error) { return 1234, nil }
	uploader := NewUploader(sc, dp, 100*time.Millisecond, UploadNoCompress)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-done
	if exp, got := int64(1), stats.Get(numUploadsSkippedID).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsSkippedSum to be %d, got %d", exp, got)
	}
}

func Test_UploaderDoubleUpload(t *testing.T) {
	ResetStats()
	var uploadedData []byte
	var err error

	var wg sync.WaitGroup
	wg.Add(2)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			defer wg.Done()
			uploadedData = nil // Wipe out any previous state.
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	lastIndexCalled := false
	dp.lastIndexFn = func() (uint64, error) {
		if lastIndexCalled {
			return 2, nil
		}
		lastIndexCalled = true
		return 1, nil
	}
	uploader := NewUploader(sc, dp, 100*time.Millisecond, UploadNoCompress)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-done
	if exp, got := "my upload data", string(uploadedData); exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}
}

func Test_UploaderFailThenOK(t *testing.T) {
	ResetStats()
	var uploadedData []byte
	uploadCount := 0
	var err error

	var wg sync.WaitGroup
	wg.Add(2)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			if uploadCount == 2 {
				// uploadFn can be called a third time before the cancel kicks in.
				// This would push waitGroup into negative numbers and panic.
				return nil
			}
			defer wg.Done()
			if uploadCount == 0 {
				uploadCount++
				return fmt.Errorf("failed to upload")
			}

			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	lastIndexCalled := false
	dp.lastIndexFn = func() (uint64, error) {
		if lastIndexCalled {
			return 2, nil
		}
		lastIndexCalled = true
		return 1, nil
	}
	uploader := NewUploader(sc, dp, 100*time.Millisecond, UploadNoCompress)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-done
	if exp, got := "my upload data", string(uploadedData); exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}
}

func Test_UploaderOKThenFail(t *testing.T) {
	ResetStats()
	var uploadedData []byte
	uploadCount := 0
	var err error

	var wg sync.WaitGroup
	wg.Add(2)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			if uploadCount == 2 {
				// uploadFn can be called a third time before the cancel kicks in.
				// This would push waitGroup into negative numbers and panic.
				return nil
			}
			defer wg.Done()

			if uploadCount == 1 {
				return fmt.Errorf("failed to upload")
			}

			uploadCount++
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	lastIndexCalled := false
	dp.lastIndexFn = func() (uint64, error) {
		if lastIndexCalled {
			return 2, nil
		}
		lastIndexCalled = true
		return 1, nil
	}
	uploader := NewUploader(sc, dp, 100*time.Millisecond, UploadNoCompress)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-done
	if exp, got := "my upload data", string(uploadedData); exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}
}

func Test_UploaderContextCancellation(t *testing.T) {
	ResetStats()
	var uploadCount int32

	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			atomic.AddInt32(&uploadCount, 1)
			return nil
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond, UploadNoCompress)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)

	done := uploader.Start(ctx, nil)
	cancel()
	<-done
	if exp, got := int32(0), atomic.LoadInt32(&uploadCount); exp != got {
		t.Errorf("expected uploadCount to be %d, got %d", exp, got)
	}
}

func Test_UploaderEnabledFalse(t *testing.T) {
	ResetStats()
	sc := &mockStorageClient{}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond, false)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, func() bool { return false })
	time.Sleep(time.Second)

	if exp, got := int64(0), stats.Get(numUploadsOK).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsOK to be %d, got %d", exp, got)
	}
	cancel()
	<-done
}

func Test_UploaderEnabledTrue(t *testing.T) {
	ResetStats()
	var uploadedData []byte
	var err error

	var wg sync.WaitGroup
	wg.Add(1)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader, id string) error {
			defer wg.Done()
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, time.Second, UploadNoCompress)
	ctx, cancel := context.WithCancel(context.Background())

	done := uploader.Start(ctx, func() bool { return true })
	wg.Wait()
	if exp, got := string(uploadedData), "my upload data"; exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}
	cancel()
	<-done
}

func Test_UploaderStats(t *testing.T) {
	ResetStats()
	sc := &mockStorageClient{}
	dp := &mockDataProvider{data: "my upload data"}
	interval := 100 * time.Millisecond
	uploader := NewUploader(sc, dp, interval, UploadNoCompress)

	stats, err := uploader.Stats()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if exp, got := sc.String(), stats["upload_destination"]; exp != got {
		t.Errorf("expected upload_destination to be %s, got %s", exp, got)
	}

	if exp, got := interval.String(), stats["upload_interval"]; exp != got {
		t.Errorf("expected upload_interval to be %s, got %s", exp, got)
	}
}

// mockStorageClient implements StorageClient and in its default configuration
// always returns an error for CurrentSum.
type mockStorageClient struct {
	uploadFn    func(ctx context.Context, reader io.Reader, id string) error
	currentIDFn func(ctx context.Context) (string, error)
}

func (mc *mockStorageClient) Upload(ctx context.Context, reader io.Reader, id string) error {
	if mc.uploadFn != nil {
		return mc.uploadFn(ctx, reader, id)
	}
	return nil
}

func (mc *mockStorageClient) CurrentID(ctx context.Context) (string, error) {
	if mc.currentIDFn != nil {
		return mc.currentIDFn(ctx)
	}
	return "", fmt.Errorf("no current sum")
}

func (mc *mockStorageClient) String() string {
	return "mockStorageClient"
}

// mockDataProvider implements DataProvider and in its default configuration
// always returns the current time for LastModified and writes the data it is
// provided to the file specified by Provide.
type mockDataProvider struct {
	data        string
	err         error
	lastIndexFn func() (uint64, error)
}

func (mp *mockDataProvider) LastIndex() (uint64, error) {
	if mp.lastIndexFn != nil {
		return mp.lastIndexFn()
	}
	return 1, nil
}

func (mp *mockDataProvider) Provide(w io.Writer) error {
	if mp.err != nil {
		return mp.err
	}

	if _, err := w.Write([]byte(mp.data)); err != nil {
		return err
	}
	return nil
}

func mustWriteToFile(s string) string {
	f, err := os.CreateTemp("", "rqlite-auto-backup-test")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := os.WriteFile(f.Name(), []byte(s), 0644); err != nil {
		panic(err)
	}
	return f.Name()
}
