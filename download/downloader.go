package download

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
)

// StorageClient is an interface for downloading data from a storage service.
type StorageClient interface {
	Download(ctx context.Context, writer io.WriterAt) error
	fmt.Stringer
}

// stats captures stats for the Uploader service.
var stats *expvar.Map

const (
	numDownloadsOK   = "num_downloads_ok"
	numDownloadsFail = "num_downloads_fail"
	numDownloadBytes = "download_bytes"
)

func init() {
	stats = expvar.NewMap("downloader")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numDownloadsOK, 0)
	stats.Add(numDownloadsFail, 0)
	stats.Add(numDownloadBytes, 0)
}

type Downloader struct {
	storageClient StorageClient
	logger        *log.Logger
}

func NewDownloader(storageClient StorageClient) *Downloader {
	return &Downloader{
		storageClient: storageClient,
		logger:        log.New(os.Stderr, "[downloader] ", log.LstdFlags),
	}
}

func (d *Downloader) Do(ctx context.Context, writer io.WriterAt) error {
	counterW := &countingWriterAt{writerAt: writer}

	err := d.storageClient.Download(ctx, counterW)
	if err != nil {
		stats.Add(numDownloadsFail, 1)
		d.logger.Printf("download failed: %v", err)
		return err
	}

	// Do compression check here, and count.

	stats.Add(numDownloadsOK, 1)
	stats.Add(numDownloadBytes, counterW.count)
	return nil
}

type countingWriterAt struct {
	writerAt io.WriterAt
	count    int64
}

func (c *countingWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = c.writerAt.WriteAt(p, off)
	c.count += int64(n)
	return
}
