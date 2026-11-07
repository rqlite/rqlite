package restore

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v10/auto/crypto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// stats captures stats for the Downloader service.
var stats *expvar.Map

var (
	gzipMagic = []byte{0x1f, 0x8b, 0x08}
	ageMagic  = []byte("age-encryption.org/v1\n")
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
				fsutil.Remove(f.Name())
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
	d := NewDownloader(sc)
	d.SetDecryptionKeys(dCfg.DecryptionKeys)

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

// Downloader is a struct that handles downloading data from a storage service.
type Downloader struct {
	storageClient  StorageClient
	logger         *log.Logger
	decryptionKeys string
}

// NewDownloader creates a new Downloader instance with the given StorageClient.
func NewDownloader(storageClient StorageClient) *Downloader {
	return &Downloader{
		storageClient: storageClient,
		logger:        log.New(os.Stderr, "[downloader] ", log.LstdFlags),
	}
}

// SetDecryptionKeys sets the age private identities used to decrypt downloads.
// An empty string disables decryption. Call this before Do.
func (d *Downloader) SetDecryptionKeys(keys string) {
	d.decryptionKeys = keys
}

// Do downloads data from the storage service and writes it to the provided writer.
// If decryption fails, earlier authenticated chunks may already have been written.
// The caller must discard the output if Do returns an error.
func (d *Downloader) Do(ctx context.Context, w io.Writer, timeout time.Duration) (err error) {
	var cw *countingWriterAt
	defer func() {
		if err == nil {
			stats.Add(numDownloadsOK, 1)
			if cw != nil {
				stats.Add(numDownloadBytes, cw.count)
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
	defer fsutil.Remove(f.Name())
	defer f.Close()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cw = &countingWriterAt{writerAt: f}
	err = d.storageClient.Download(ctx, cw)
	if err != nil {
		return err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(f)
	if d.decryptionKeys != "" {
		decrypted, err := crypto.NewDecryptReader(r, d.decryptionKeys)
		if err != nil {
			return fmt.Errorf("failed to decrypt data: %w", err)
		}
		r = bufio.NewReader(decrypted)
	} else {
		encrypted, err := isAge(r)
		if err != nil {
			return err
		}
		if encrypted {
			return fmt.Errorf("backup is encrypted, but no decryption_keys configured")
		}
	}

	// Check for gzip after decryption, without consuming the stream prefix.
	compressed, err := isGzip(r)
	if err != nil {
		return err
	}

	if compressed {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			return err
		}
		defer gzr.Close()

		// Keep gzip's default multistream mode so EOF also requires consuming
		// the underlying stream, including age's final authentication check.
		_, err = io.Copy(w, gzr)
		if err != nil {
			return fmt.Errorf("failed to decompress data: %s", err)
		}
	} else {
		_, err = io.Copy(w, r)
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

// isGzip reports whether the stream starts with the gzip magic bytes without
// consuming them.
func isGzip(r *bufio.Reader) (bool, error) {
	data, err := r.Peek(len(gzipMagic))
	if err != nil && err != io.EOF {
		return false, err
	}
	return bytes.Equal(gzipMagic, data), nil
}

// isAge reports whether the stream starts with the age header without consuming it.
func isAge(r *bufio.Reader) (bool, error) {
	data, err := r.Peek(len(ageMagic))
	if err != nil && err != io.EOF {
		return false, err
	}
	return bytes.Equal(ageMagic, data), nil
}
