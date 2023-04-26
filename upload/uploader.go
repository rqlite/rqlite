package upload

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type StorageClient interface {
	Upload(ctx context.Context, reader io.Reader) error
	fmt.Stringer
}

type DataProvider interface {
	Provide() (io.Reader, error)
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numUploadsOKKey  = "num_uploads_ok"
	numUploadsFail   = "num_uploads_fail"
	totalUploadBytes = "total_upload_bytes"
	lastUploadBytes  = "last_upload_bytes"
)

func init() {
	stats = expvar.NewMap("uploader")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numUploadsOKKey, 0)
	stats.Add(numUploadsFail, 0)
	stats.Add(totalUploadBytes, 0)
	stats.Add(lastUploadBytes, 0)
}

type Uploader struct {
	storageClient StorageClient
	dataProvider  DataProvider
	interval      time.Duration

	logger *log.Logger
}

func NewUploader(storageClient StorageClient, dataProvider DataProvider, interval time.Duration) *Uploader {
	return &Uploader{
		storageClient: storageClient,
		dataProvider:  dataProvider,
		interval:      interval,
		logger:        log.New(os.Stderr, "[uploader] ", log.LstdFlags),
	}
}

func (u *Uploader) Start(ctx context.Context, bucket, path string) {
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := u.upload(ctx); err != nil {
				u.logger.Printf("failed to upload to %s: %v", u.storageClient, err)
			}
		}
	}
}

func (u *Uploader) upload(ctx context.Context) error {
	reader, err := u.dataProvider.Provide()
	if err != nil {
		return err
	}
	cr := &countingReader{reader: reader}
	err = u.storageClient.Upload(context.Background(), cr)
	if err != nil {
		stats.Add(numUploadsFail, 1)
	} else {
		stats.Add(numUploadsOKKey, 1)
		stats.Add(totalUploadBytes, cr.count)
		stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.count)
	}
	return err
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.count += int64(n)
	return n, err
}
