package backup

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/rqlite/rqlite/v8/db/humanize"
	"github.com/rqlite/rqlite/v8/progress"
)

// StorageClient is an interface for uploading data to a storage service.
type StorageClient interface {
	// Upload uploads the data from the given reader to the storage service.
	// id is a identifier for the data, and will be stored along with
	// the data in the storage service.
	Upload(ctx context.Context, reader io.Reader, id string) error

	// CurrentID returns the ID of the data in the Storage service.
	// It is always read from the Storage service, and a cached
	// value is never returned.
	CurrentID(ctx context.Context) (string, error)

	fmt.Stringer
}

// DataProvider is an interface for providing data to be uploaded. The Uploader
// service will call Provide() to have the data-for-upload to be written to the
// to the file specified by path.
type DataProvider interface {
	// LastIndex returns the cluster-wide index the data managed by the DataProvider was
	// last modified by.
	LastIndex() (uint64, error)

	// Provide writes the data-for-upload to the writer.
	Provide(w io.Writer) error
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numUploadsOK        = "num_uploads_ok"
	numUploadsFail      = "num_uploads_fail"
	numUploadsSkipped   = "num_uploads_skipped"
	numUploadsSkippedID = "num_uploads_skipped_id"
	numSumGetFail       = "num_sum_get_fail"
	totalUploadBytes    = "total_upload_bytes"
	lastUploadBytes     = "last_upload_bytes"
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
	stats.Add(numUploadsSkippedID, 0)
	stats.Add(numSumGetFail, 0)
	stats.Add(totalUploadBytes, 0)
	stats.Add(lastUploadBytes, 0)
}

// Uploader is a service that periodically uploads data to a storage service.
type Uploader struct {
	storageClient StorageClient
	dataProvider  DataProvider
	interval      time.Duration

	logger             *log.Logger
	lastUploadTime     time.Time
	lastUploadDuration time.Duration

	lastIndex uint64 // The last index of the data most-recently uploaded.
}

// NewUploader creates a new Uploader service.
func NewUploader(storageClient StorageClient, dataProvider DataProvider, interval time.Duration) *Uploader {
	return &Uploader{
		storageClient: storageClient,
		dataProvider:  dataProvider,
		interval:      interval,
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
func (u *Uploader) Stats() (map[string]any, error) {
	status := map[string]any{
		"upload_destination":   u.storageClient.String(),
		"upload_interval":      u.interval.String(),
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
		"last_index":           strconv.FormatUint(u.lastIndex, 10),
	}
	return status, nil
}

func (u *Uploader) upload(ctx context.Context) error {
	var err error
	var li uint64

	li, err = u.dataProvider.LastIndex()
	if err != nil {
		return err
	}
	if li <= u.lastIndex {
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

	if err := u.dataProvider.Provide(fd); err != nil {
		return err
	}

	if u.lastIndex == 0 {
		// No last index, so this must be the first upload since this
		// uploader started. Double-check that we really need to upload.
		cloudID, err := u.storageClient.CurrentID(ctx)
		if err != nil {
			stats.Add(numSumGetFail, 1)
			u.logger.Printf("failed to get current sum from %s: %v", u.storageClient, err)
		} else if cloudID == strconv.FormatUint(li, 10) {
			stats.Add(numUploadsSkippedID, 1)
			return nil
		}
	}

	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	cr := progress.NewCountingReader(fd)
	startTime := time.Now()
	err = u.storageClient.Upload(ctx, cr, strconv.FormatUint(li, 10))
	if err != nil {
		stats.Add(numUploadsFail, 1)
		return err
	}

	// Successful upload!
	u.lastIndex = li
	stats.Add(numUploadsOK, 1)
	stats.Add(totalUploadBytes, cr.Count())
	stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.Count())
	u.lastUploadTime = time.Now()
	u.lastUploadDuration = time.Since(startTime)
	u.logger.Printf("completed auto upload of %s to %s in %s",
		humanize.Bytes(uint64(stats.Get(lastUploadBytes).(*expvar.Int).Value())),
		u.storageClient, u.lastUploadDuration)
	return nil
}

func tempFD() (*os.File, error) {
	return os.CreateTemp("", "rqlite-upload")
}
