package upload

import (
	"bytes"
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
// service will call Provide() to get a reader for the data to be uploaded. Once
// the upload completes the reader will be closed, regardless of whether the
// upload succeeded or failed.
type DataProvider interface {
	Provide() (io.ReadCloser, error)
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numUploadsOK     = "num_uploads_ok"
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
	stats.Add(numUploadsOK, 0)
	stats.Add(numUploadsFail, 0)
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
func (u *Uploader) Start(ctx context.Context, enabled func() bool) {
	u.logger.Printf("starting upload to %s every %s", u.storageClient, u.interval)
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			u.logger.Println("upload service shutting down")
			return
		case <-ticker.C:
			if enabled != nil && !enabled() {
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
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
	}
	return status, nil
}

func (u *Uploader) upload(ctx context.Context) error {
	rc, err := u.dataProvider.Provide()
	if err != nil {
		return err
	}
	defer rc.Close()

	cr := &countingReader{reader: rc}
	startTime := time.Now()
	err = u.storageClient.Upload(ctx, cr)
	if err != nil {
		stats.Add(numUploadsFail, 1)
	} else {
		stats.Add(numUploadsOK, 1)
		stats.Add(totalUploadBytes, cr.count)
		stats.Get(lastUploadBytes).(*expvar.Int).Set(cr.count)
		u.lastUploadTime = time.Now()
		u.lastUploadDuration = time.Since(startTime)
	}
	return err
}

type GZIPReader struct {
	src         io.Reader
	gw          *gzip.Writer
	buffer      *bytes.Buffer
	compressed  *bytes.Buffer
	chunkSize   int64
}

func NewGZIPReader(src io.Reader, chunkSize int64) *GZIPReader {
	buffer := new(bytes.Buffer)
	gw := gzip.NewWriter(buffer)

	return &GZIPReader{
		src:         src,
		gw:          gw,
		buffer:      buffer,
		compressed:  new(bytes.Buffer),
		chunkSize:   chunkSize,
	}
}

func (g *GZIPReader) compressChunk() error {
	g.buffer.Reset()

	_, err := io.CopyN(g.gw, g.src, g.chunkSize)
	if err != nil && err != io.EOF {
		return fmt.Errorf("error copying data to gzip writer: %w", err)
	}

	if err := g.gw.Flush(); err != nil {
		return fmt.Errorf("error flushing gzip writer: %w", err)
	}

	_, err = io.Copy(g.compressed, g.buffer)
	return err
}

func (g *GZIPReader) Read(p []byte) (int, error) {
	for g.compressed.Len() < len(p) {
		err := g.compressChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
	}

	return g.compressed.Read(p)
}

func (g *GZIPReader) Close() error {
	return g.gw.Close()
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
