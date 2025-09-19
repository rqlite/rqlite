package file

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config represents configuration for the file storage client.
type Config struct {
	Dir  string `json:"dir"`
	File string `json:"file"`
}

// Client represents a file storage client.
type Client struct {
	dir       string
	file      string
	metaPath  string
	timestamp bool
	logger    *log.Logger
}

// Options represents options for the file storage client.
type Options struct {
	Timestamp bool
	Logger    *log.Logger
}

// Metadata represents metadata stored in the metadata file.
type Metadata struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp,omitempty"`
	File      string `json:"file,omitempty"`
}

// NewClient creates a new file storage client.
func NewClient(dir, file string, opt *Options) (*Client, error) {
	// Validate and clean paths
	dir = filepath.Clean(dir)
	if !filepath.IsAbs(dir) && !strings.HasPrefix(dir, ".") {
		// If not absolute and not relative with ., make it relative to current dir
		dir = "./" + dir
	}

	// Validate file parameter for path traversal attacks and directory separators
	cleanFile := filepath.Clean(file)
	if strings.Contains(file, string(filepath.Separator)) || strings.Contains(cleanFile, "..") || filepath.IsAbs(cleanFile) || cleanFile != file {
		return nil, fmt.Errorf("invalid file parameter: %s (must be a simple filename without path separators)", file)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	touchPath := filepath.Join(dir, ".touch")
	f, err := os.OpenFile(touchPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to test writing to directory %s: %w", dir, err)
	}
	f.Close()
	os.Remove(touchPath)

	c := &Client{
		dir:      dir,
		file:     file,
		metaPath: filepath.Join(dir, "METADATA.json"),
		logger:   log.New(os.Stderr, "[auto-backup-file] ", log.LstdFlags),
	}

	if opt != nil {
		c.timestamp = opt.Timestamp
		if opt.Logger != nil {
			c.logger = opt.Logger
		}
	}
	return c, nil
}

// CurrentMetadata returns the current metadata.
func (c *Client) CurrentMetadata(ctx context.Context) (*Metadata, error) {
	if !fileExists(c.metaPath) {
		return nil, nil
	}
	data, err := os.ReadFile(c.metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", c.metaPath, err)
	}
	var md Metadata
	if err := json.Unmarshal(data, &md); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata from file %s: %w", c.metaPath, err)
	}
	return &md, nil
}

// LatestFilePath returns the path to the most recently uploaded file.
func (c *Client) LatestFilePath(ctx context.Context) string {
	md, err := c.CurrentMetadata(ctx)
	if err != nil {
		return ""
	}
	if md == nil {
		return ""
	}
	return md.File
}

// String returns a string representation of the client.
func (c *Client) String() string {
	return fmt.Sprintf("dir:%s", c.dir)
}

// Upload uploads data from the reader to the file storage.
func (c *Client) Upload(ctx context.Context, reader io.Reader, id string) (retErr error) {
	// Determine final filename
	filename := c.file
	if c.timestamp {
		// Use millisecond precision for timestamp to avoid collisions
		ts := time.Now().UTC().Format("20060102150405.000")
		// Split filename into base name and extension
		ext := filepath.Ext(filename)
		base := strings.TrimSuffix(filename, ext)
		filename = fmt.Sprintf("%s_%s%s", ts, base, ext)
	}

	finalPath := filepath.Join(c.dir, filename)

	c.logger.Printf("starting upload: raft_id=%s timestamp=%t path=%s", id, c.timestamp, finalPath)

	// Use os.CreateTemp for better atomic temporary file creation
	tmpFile, err := os.CreateTemp(c.dir, ".upload-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file in %s: %w", c.dir, err)
	}
	tmpPath := tmpFile.Name()

	tmpMetaPath := c.metaPath + ".tmp"

	// Cleanup on error
	defer func() {
		tmpFile.Close()
		if retErr != nil {
			os.Remove(tmpMetaPath)
			os.Remove(tmpPath)
		}
	}()

	// Write data to temporary file
	startTime := time.Now()
	n, err := io.Copy(tmpFile, reader)
	if err != nil {
		return fmt.Errorf("failed to write to temporary file %s: %w", tmpPath, err)
	}

	// Fsync to ensure data is written to disk
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file %s: %w", tmpPath, err)
	}

	// Close temp file before rename
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file %s: %w", tmpPath, err)
	}

	// Write metadata to temporary metadata file
	metadata := Metadata{
		ID:        id,
		Timestamp: time.Now().UnixMilli(),
		File:      finalPath,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(tmpMetaPath, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write temporary metadata file %s: %w", tmpMetaPath, err)
	}

	// Atomic renames (on same filesystem)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename temporary file %s to %s: %w", tmpPath, finalPath, err)
	}

	if err := os.Rename(tmpMetaPath, c.metaPath); err != nil {
		// Try to clean up the data file if metadata rename fails
		os.Remove(finalPath)
		return fmt.Errorf("failed to rename temporary metadata file %s to %s: %w", tmpMetaPath, c.metaPath, err)
	}

	elapsed := time.Since(startTime)
	c.logger.Printf("completed upload: raft_id=%s bytes=%d path=%s elapsed=%v", id, n, finalPath, elapsed)

	return nil
}

// CurrentID returns the current ID stored in the metadata.
func (c *Client) CurrentID(ctx context.Context) (string, error) {
	md, err := c.CurrentMetadata(ctx)
	if err != nil {
		return "", err
	}
	if md == nil {
		return "", nil
	}
	return md.ID, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
