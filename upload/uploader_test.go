package upload

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Test_NewUploader(t *testing.T) {
	storageClient := &mockStorageClient{}
	dataProvider := &mockDataProvider{}
	interval := time.Second
	uploader := NewUploader(storageClient, dataProvider, interval)
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
		uploadFn: func(ctx context.Context, reader io.Reader) error {
			defer wg.Done()
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	go uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-ctx.Done()

	if exp, got := string(uploadedData), "my upload data"; exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}

	// Check the metrics.
	if exp, got := int64(1), stats.Get(numUploadsOK).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsOK to be %d, got %d", exp, got)
	}
	if exp, got := int64(0), stats.Get(numUploadsFail).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsFail to be %d, got %d", exp, got)
	}
	if exp, got := int64(len("my upload data")), stats.Get(totalUploadBytes).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected totalUploadBytes to be %d, got %d", exp, got)
	}
	if exp, got := int64(len("my upload data")), stats.Get(lastUploadBytes).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected lastUploadBytes to be %d, got %d", exp, got)
	}
}

func Test_UploaderDoubleUpload(t *testing.T) {
	ResetStats()

	var uploadedData []byte
	var err error

	var wg sync.WaitGroup
	wg.Add(2)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader) error {
			defer wg.Done()
			uploadedData = nil // Wipe out any previous state.
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	go uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-ctx.Done()

	if exp, got := string(uploadedData), "my upload data"; exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}

	// Check the metrics.
	if exp, got := int64(2), stats.Get(numUploadsOK).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsOK to be %d, got %d", exp, got)
	}
	if exp, got := int64(0), stats.Get(numUploadsFail).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsFail to be %d, got %d", exp, got)
	}
	if exp, got := int64(2*len("my upload data")), stats.Get(totalUploadBytes).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected totalUploadBytes to be %d, got %d", exp, got)
	}
	if exp, got := int64(len("my upload data")), stats.Get(lastUploadBytes).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected lastUploadBytes to be %d, got %d", exp, got)
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
		uploadFn: func(ctx context.Context, reader io.Reader) error {
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
	uploader := NewUploader(sc, dp, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	go uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-ctx.Done()

	if exp, got := string(uploadedData), "my upload data"; exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}

	// Check the metrics.
	if exp, got := int64(1), stats.Get(numUploadsOK).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsOK to be %d, got %d", exp, got)
	}
	if exp, got := int64(1), stats.Get(numUploadsFail).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsFail to be %d, got %d", exp, got)
	}
	if exp, got := int64(len("my upload data")), stats.Get(totalUploadBytes).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected totalUploadBytes to be %d, got %d", exp, got)
	}
	if exp, got := int64(len("my upload data")), stats.Get(lastUploadBytes).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected lastUploadBytes to be %d, got %d", exp, got)
	}
}

func Test_UploaderOKThenFail(t *testing.T) {
	var uploadedData []byte
	uploadCount := 0
	var err error

	var wg sync.WaitGroup
	wg.Add(2)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader) error {
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
	uploader := NewUploader(sc, dp, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	go uploader.Start(ctx, nil)
	wg.Wait()
	cancel()
	<-ctx.Done()

	if exp, got := string(uploadedData), "my upload data"; exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}
}

func Test_UploaderContextCancellation(t *testing.T) {
	var uploadCount int32

	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader) error {
			atomic.AddInt32(&uploadCount, 1)
			return nil
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)

	go uploader.Start(ctx, nil)
	<-ctx.Done()
	cancel()
	<-ctx.Done()

	if exp, got := int32(0), atomic.LoadInt32(&uploadCount); exp != got {
		t.Errorf("expected uploadCount to be %d, got %d", exp, got)
	}
}

func Test_UploaderEnabledFalse(t *testing.T) {
	ResetStats()

	sc := &mockStorageClient{}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	go uploader.Start(ctx, func() bool { return false })
	time.Sleep(time.Second)
	defer cancel()

	if exp, got := int64(0), stats.Get(numUploadsOK).(*expvar.Int); exp != got.Value() {
		t.Errorf("expected numUploadsOK to be %d, got %d", exp, got)
	}
}

func Test_UploaderEnabledTrue(t *testing.T) {
	var uploadedData []byte
	var err error
	ResetStats()

	var wg sync.WaitGroup
	wg.Add(1)
	sc := &mockStorageClient{
		uploadFn: func(ctx context.Context, reader io.Reader) error {
			defer wg.Done()
			uploadedData, err = io.ReadAll(reader)
			return err
		},
	}
	dp := &mockDataProvider{data: "my upload data"}
	uploader := NewUploader(sc, dp, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	go uploader.Start(ctx, func() bool { return true })
	defer cancel()

	wg.Wait()
	if exp, got := string(uploadedData), "my upload data"; exp != got {
		t.Errorf("expected uploadedData to be %s, got %s", exp, got)
	}
}

func Test_UploaderStats(t *testing.T) {
	sc := &mockStorageClient{}
	dp := &mockDataProvider{data: "my upload data"}
	interval := 100 * time.Millisecond
	uploader := NewUploader(sc, dp, interval)

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

type mockStorageClient struct {
	uploadFn func(ctx context.Context, reader io.Reader) error
}

func (mc *mockStorageClient) Upload(ctx context.Context, reader io.Reader) error {
	if mc.uploadFn != nil {
		return mc.uploadFn(ctx, reader)
	}
	return nil
}

func (mc *mockStorageClient) String() string {
	return "mockStorageClient"
}

type mockDataProvider struct {
	data string
	err  error
}

func (mp *mockDataProvider) Provide() (io.ReadCloser, error) {
	if mp.err != nil {
		return nil, mp.err
	}
	return io.NopCloser(strings.NewReader(mp.data)), nil
}
