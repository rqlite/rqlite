package restore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/aws"
	"github.com/rqlite/rqlite/v8/gcp"
)

// stats captures stats for the Uploader service.
var stats *expvar.Map

var (
	gzipMagic = []byte{0x1f, 0x8b, 0x08}
)

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

// DownloadFile downloads the auto-restore file from the given URL, and returns the path to
// the downloaded file. If the download fails, and the config is marked as continue-on-failure, then
// the error is returned, but errOK is set to true. If the download fails, and the file is not
// marked as continue-on-failure, then the error is returned, and errOK is set to false.
func DownloadFile(ctx context.Context, cfgPath string) (path string, errOK bool, err error) {
	var f *os.File
	defer func() {
		if err != nil {
			if f != nil {
				f.Close()
				os.Remove(f.Name())
			}
		}
	}()

	b, err := ReadConfigFile(cfgPath)
	if err != nil {
		return "", false, fmt.Errorf("failed to read auto-restore file: %s", err.Error())
	}

	var sc StorageClient
	var dCfg *Config
	dCfg, sc, err = NewStorageClient(b)
	if err != nil {
		return "", false, fmt.Errorf("failed to create storage client: %s", err.Error())
	}

	switch dCfg.Type {
	case auto.StorageTypeS3:
		s3cfg := &aws.S3Config{}
		err = json.Unmarshal(dCfg.Sub, s3cfg)
		if err != nil {
			return "", false, err
		}
		s3ClientOps := &aws.S3ClientOpts{
			ForcePathStyle: s3cfg.ForcePathStyle,
		}
		sc, err = aws.NewS3Client(s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey,
			s3cfg.Bucket, s3cfg.Path, s3ClientOps)
	case auto.StorageTypeGCS:
		gcsCfg := &gcp.GCSConfig{}
		err = json.Unmarshal(dCfg.Sub, gcsCfg)
		if err != nil {
			return "", false, err
		}
		sc, err = gcp.NewGCSClient(gcsCfg)
	default:
		return "", false, auto.ErrUnsupportedStorageType
	}

	d := NewDownloader(sc)

	// Create a temporary file to download to.
	f, err = os.CreateTemp("", "rqlite-auto-restore")
	if err != nil {
		return "", false, fmt.Errorf("failed to create temporary file: %s", err.Error())
	}
	defer f.Close()

	if err := d.Do(ctx, f, time.Duration(dCfg.Timeout)); err != nil {
		return "", dCfg.ContinueOnFailure, fmt.Errorf("failed to download auto-restore file: %s", err.Error())
	}
	return f.Name(), false, nil
}

// StorageClient is an interface for downloading data from a storage service.
type StorageClient interface {
	Download(ctx context.Context, writer io.WriterAt) error
	fmt.Stringer
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

func (d *Downloader) Do(ctx context.Context, w io.Writer, timeout time.Duration) (err error) {
	var cw *countingWriterAt
	defer func() {
		if err == nil {
			stats.Add(numDownloadsOK, 1)
			if cw != nil {
				stats.Add(numDownloadBytes, int64(cw.count))
			}
		} else {
			stats.Add(numDownloadsFail, 1)
		}
	}()

	// Create a temporary file for the download.
	f, err := os.CreateTemp("", "rqlite-downloader")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cw = &countingWriterAt{writerAt: f}
	err = d.storageClient.Download(ctx, cw)
	if err != nil {
		return err
	}

	// Check if the download data is gzip compressed.
	compressed, err := isGzip(f)
	if err != nil {
		return err
	}

	if compressed {
		gzr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gzr.Close()

		_, err = io.Copy(w, gzr)
		if err != nil {
			return fmt.Errorf("failed to decompress data: %s", err)
		}
	} else {
		_, err = io.Copy(w, f)
		if err != nil {
			return fmt.Errorf("failed to write data: %s", err)
		}
	}
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

// isGzip returns true if the data in the reader is gzip compressed.
// It does this by reading the first three bytes of the reader, and checking
// if they match the gzip magic number. When f is returned it will be
// positioned at the start of the reader.
func isGzip(f io.ReadSeeker) (bool, error) {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return false, err
	}
	data := make([]byte, len(gzipMagic))
	n, err := f.Read(data)
	if err != nil {
		return false, err
	}
	if n != len(gzipMagic) {
		return false, nil
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return false, err
	}

	return bytes.Equal(gzipMagic, data[0:len(gzipMagic)]), nil
}
