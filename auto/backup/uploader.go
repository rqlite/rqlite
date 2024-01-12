package backup

import (
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
	// Upload uploads the data from the given reader to the storage service.
	// sum is the SHA256 sum of the data being uploaded and will be stored with
	// the data. If sum is nil, no sum will be stored.
	Upload(ctx context.Context, reader io.Reader, sum []byte) error

	// CurrentSum returns the SHA256 sum of the data in the Storage service.
	// It is always read from the Storage service and never cached.
	CurrentSum(ctx context.Context) ([]byte, error)

	fmt.Stringer
}

// DataProvider is an interface for providing data to be uploaded. The Uploader
// service will call Provide() to have the data-for-upload to be written to the
// to the file specified by path.
type DataProvider interface {
	// LastModified returns the time the data managed by the DataProvider was
	// last modified.
	LastModified() (time.Time, error)

	// Provide writes the data-for-upload to the writer. Because Provide may
	// change the data in the DataProvider, it returns the current modified
	// time of the data, after the data has been written to path.
	Provide(w io.Writer) (time.Time, error)
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numUploadsOK         = "num_uploads_ok"
	numUploadsFail       = "num_uploads_fail"
	numUploadsSkipped    = "num_uploads_skipped"
	numUploadsSkippedSum = "num_uploads_skipped_sum"
	numSumGetFail        = "num_sum_get_fail"
	totalUploadBytes     = "total_upload_bytes"
	lastUploadBytes      = "last_upload_bytes"

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
	stats.Add(numUploadsSkippedSum, 0)
	stats.Add(numSumGetFail, 0)
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

	lastSumUploaded []byte
	lastModified    time.Time // The last-modified time of the data most-recently uploaded.
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
		"last_sum_uploaded":    fmt.Sprintf("%x", u.lastSumUploaded),
		"last_modified":        u.lastModified.String(),
	}
	return status, nil
}

func (u *Uploader) upload(ctx context.Context) error {
	var err error
	var lm time.Time

	lm, err = u.dataProvider.LastModified()
	if err != nil {
		return err
	}
	if !lm.After(u.lastModified) {
		stats.Add(numUploadsSkipped, 1)
		return nil
	}

	// Create a temporary file for the data to be uploaded
	fd, err := tempFD()
	if err != nil {
		return err
	}
	defer os.Remove(fd.Name())
	defer fd.Close()

	lm, err = u.dataProvider.Provide(fd)
	if err != nil {
		return err
	}

	filesum, err := FileSHA256(fd.Name())
	if err != nil {
		return err
	}
	if u.lastModified.IsZero() {
		// No "last modified" time, so this must be the first upload since this
		// uploader started. Double-check that we really need to upload.
		cloudSum, err := u.currentSum(ctx)
		if err != nil {
			stats.Add(numSumGetFail, 1)
			u.logger.Printf("failed to get current sum from %s: %v", u.storageClient, err)
		} else if err == nil && filesum.Equals(cloudSum) {
			stats.Add(numUploadsSkippedSum, 1)
			return nil
		}
	}

	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	cr := progress.NewCountingReader(fd)
	startTime := time.Now()
	err = u.storageClient.Upload(ctx, cr, filesum)
	if err != nil {
		stats.Add(numUploadsFail, 1)
	} else {
		u.lastSumUploaded = filesum
		u.lastModified = lm
		stats.Add(numUploadsOK, 1)
		stats.Add(totalUploadBytes, cr.Count())
		stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.Count())
		u.lastUploadTime = time.Now()
		u.lastUploadDuration = time.Since(startTime)
		u.logger.Printf("completed auto upload to %s in %s", u.storageClient, u.lastUploadDuration)
	}
	return err
}

func (u *Uploader) currentSum(ctx context.Context) (SHA256Sum, error) {
	s, err := u.storageClient.CurrentSum(ctx)
	if err != nil {
		return nil, err
	}
	return SHA256Sum(s), nil
}

func tempFD() (*os.File, error) {
	return os.CreateTemp("", "rqlite-upload")
}
