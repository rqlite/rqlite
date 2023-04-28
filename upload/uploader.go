package upload

import (
	"compress/gzip"
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// StorageClient is an interface for uploading data to a storage service.
type StorageClient interface {
	Upload(ctx context.Context, reader io.Reader) error
	fmt.Stringer
}

// DataProvider is an interface for providing data to be uploaded. The Uploader
// service will call Provide() to have the data-for-upload to be written to the
// to the file specified by path.
type DataProvider interface {
	Provide(path string) error
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numUploadsOK      = "num_uploads_ok"
	numUploadsFail    = "num_uploads_fail"
	numUploadsSkipped = "num_uploads_skipped"
	totalUploadBytes  = "total_upload_bytes"
	lastUploadBytes   = "last_upload_bytes"

	UploadCompress   = true
	UploadNoCompress = false
)

func init() {
	stats = expvar.NewMap("uploader")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numUploadsOK, 0)
	stats.Add(numUploadsFail, 0)
	stats.Add(numUploadsSkipped, 0)
	stats.Add(totalUploadBytes, 0)
	stats.Add(lastUploadBytes, 0)
}

// Uploader is a service that periodically uploads data to a storage service.
type Uploader struct {
	storageClient StorageClient
	dataProvider  DataProvider
	interval      time.Duration
	compress      bool

	logger             *log.Logger
	lastUploadTime     time.Time
	lastUploadDuration time.Duration

	lastSum SHA256Sum

	// disableSumCheck is used for testing purposes to disable the check that
	// prevents uploading the same data twice.
	disableSumCheck bool
}

// NewUploader creates a new Uploader service.
func NewUploader(storageClient StorageClient, dataProvider DataProvider, interval time.Duration, compress bool) *Uploader {
	return &Uploader{
		storageClient: storageClient,
		dataProvider:  dataProvider,
		interval:      interval,
		compress:      compress,
		logger:        log.New(os.Stderr, "[uploader] ", log.LstdFlags),
	}
}

// Start starts the Uploader service.
func (u *Uploader) Start(ctx context.Context, isUploadEnabled func() bool) {
	if isUploadEnabled == nil {
		isUploadEnabled = func() bool { return true }
	}

	u.logger.Printf("starting upload to %s every %s", u.storageClient, u.interval)
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			u.logger.Println("upload service shutting down")
			return
		case <-ticker.C:
			if !isUploadEnabled() {
				// Reset the lastSum so that the next time we're enabled upload will
				// happen. We do this to be conservative, as we don't know what was
				// happening while upload was disabled.
				u.lastSum = nil
				continue
			}
			if err := u.upload(ctx); err != nil {
				u.logger.Printf("failed to upload to %s: %v", u.storageClient, err)
			}
		}
	}
}

// Stats returns the stats for the Uploader service.
func (u *Uploader) Stats() (map[string]interface{}, error) {
	status := map[string]interface{}{
		"upload_destination":   u.storageClient.String(),
		"upload_interval":      u.interval.String(),
		"compress":             u.compress,
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
	}
	return status, nil
}

func (u *Uploader) upload(ctx context.Context) error {
	// create a temporary file to hold the data to be uploaded
	tmpfile, err := tempFilename()
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile)

	if err := u.dataProvider.Provide(tmpfile); err != nil {
		return err
	}

	var sum SHA256Sum
	if !u.disableSumCheck {
		// Get the SHA256 sum of the file. If it is the same as the last one, then
		// there is no need to upload the file.
		sum, err = FileSHA256(tmpfile)
		if err != nil {
			return err
		}

		if sum.Equals(u.lastSum) {
			stats.Add(numUploadsSkipped, 1)
			return nil
		}
	}

	// Re-open the file for reading.
	uncompressedF, err := os.Open(tmpfile)
	if err != nil {
		return err
	}
	defer uncompressedF.Close()

	r := io.Reader(uncompressedF)
	if u.compress {
		compressedF, err := os.CreateTemp("", "rqlite-upload")
		if err != nil {
			return err
		}
		defer os.Remove(compressedF.Name())

		gw := gzip.NewWriter(compressedF)
		_, err = io.Copy(gw, uncompressedF)
		if err != nil {
			return err
		}
		err = gw.Close()
		if err != nil {
			return err
		}
		if err := compressedF.Close(); err != nil {
			return err
		}

		compressedF, err = os.Open(compressedF.Name())
		if err != nil {
			return err
		}
		defer compressedF.Close()
		r = io.Reader(compressedF)
	}

	cr := &countingReader{reader: r}
	startTime := time.Now()
	err = u.storageClient.Upload(ctx, cr)
	if err != nil {
		stats.Add(numUploadsFail, 1)
	} else {
		u.lastSum = sum
		stats.Add(numUploadsOK, 1)
		stats.Add(totalUploadBytes, cr.count)
		stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.count)
		u.lastUploadTime = time.Now()
		u.lastUploadDuration = time.Since(startTime)
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

func tempFilename() (string, error) {
	f, err := os.CreateTemp("", "rqlite-upload")
	if err != nil {
		return "", err
	}
	f.Close()
	return f.Name(), nil
}
