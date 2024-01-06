package backup

import (
	"compress/gzip"
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v8/progress"
)

// StorageClient is an interface for uploading data to a storage service.
type StorageClient interface {
	LatestHash(ctx context.Context) (string, error)
	Upload(ctx context.Context, reader io.Reader, hash string) error
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
func (u *Uploader) Start(ctx context.Context, isUploadEnabled func() bool) chan struct{} {
	doneCh := make(chan struct{})
	if isUploadEnabled == nil {
		isUploadEnabled = func() bool { return true }
	}

	u.logger.Printf("starting upload to %s every %s", u.storageClient, u.interval)
	ticker := time.NewTicker(u.interval)
	go func() {
		defer ticker.Stop()
		defer close(doneCh)
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
	}()
	return doneCh
}

// Stats returns the stats for the Uploader service.
func (u *Uploader) Stats() (map[string]interface{}, error) {
	status := map[string]interface{}{
		"upload_destination":   u.storageClient.String(),
		"upload_interval":      u.interval.String(),
		"compress":             u.compress,
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
		"last_upload_sum":      u.lastSum.String(),
	}
	return status, nil
}

func (u *Uploader) upload(ctx context.Context) error {
	// create a temporary file for the data to be uploaded
	filetoUpload, err := tempFilename()
	if err != nil {
		return err
	}
	defer os.Remove(filetoUpload)

	if err := u.dataProvider.Provide(filetoUpload); err != nil {
		return err
	}
	if err := u.compressIfNeeded(filetoUpload); err != nil {
		return err
	}

	// If we don't have a lastSum, try to get it from the storage service.
	if u.lastSum == nil {
		hash, err := u.storageClient.LatestHash(ctx)
		if err == nil && hash != "" {
			s, err := FromString(hash)
			if err == nil {
				u.lastSum = s
			}
		}
	}

	sum, err := FileSHA256(filetoUpload)
	if err != nil {
		return err
	}
	if !u.disableSumCheck && sum.Equals(u.lastSum) {
		stats.Add(numUploadsSkipped, 1)
		return nil
	}

	fd, err := os.Open(filetoUpload)
	if err != nil {
		return err
	}
	defer fd.Close()

	cr := progress.NewCountingReader(fd)
	startTime := time.Now()
	err = u.storageClient.Upload(ctx, cr, u.lastSum.String())
	if err != nil {
		stats.Add(numUploadsFail, 1)
	} else {
		u.lastSum = sum
		stats.Add(numUploadsOK, 1)
		stats.Add(totalUploadBytes, cr.Count())
		stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.Count())
		u.lastUploadTime = time.Now()
		u.lastUploadDuration = time.Since(startTime)
		u.logger.Printf("completed auto upload to %s in %s", u.storageClient, u.lastUploadDuration)
	}
	return err
}

func (u *Uploader) compressIfNeeded(path string) error {
	if !u.compress {
		return nil
	}

	compressedFile, err := tempFilename()
	if err != nil {
		return err
	}
	defer os.Remove(compressedFile)

	if err = compressFromTo(path, compressedFile); err != nil {
		return err
	}

	return os.Rename(compressedFile, path)
}

func compressFromTo(from, to string) error {
	uncompressedFd, err := os.Open(from)
	if err != nil {
		return err
	}
	defer uncompressedFd.Close()

	compressedFd, err := os.Create(to)
	if err != nil {
		return err
	}
	defer compressedFd.Close()

	gw := gzip.NewWriter(compressedFd)
	_, err = io.Copy(gw, uncompressedFd)
	if err != nil {
		return err
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	return nil
}

func tempFilename() (string, error) {
	f, err := os.CreateTemp("", "rqlite-upload")
	if err != nil {
		return "", err
	}
	f.Close()
	return f.Name(), nil
}
