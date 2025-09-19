package file

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config represents configuration for the file storage client.
type Config struct {
	Dir  string `json:"dir"`
	Name string `json:"name"`
}

// Client represents a file storage client.
type Client struct {
	dir       string
	name      string
	metaPath  string
	timestamp bool

	now func() time.Time
}

// Options represents options for the file storage client.
type Options struct {
	Timestamp bool
}

// Metadata represents metadata stored in the metadata file.
type Metadata struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Name      string `json:"name,omitempty"`
}

// NewClient creates a new file storage client.
func NewClient(dir, name string, opt *Options) (*Client, error) {
	// Validate and clean paths
	dir = filepath.Clean(dir)
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("directory path must be absolute: %s", dir)
	}

	// Validate file parameter for path traversal attacks and directory separators
	cleanFile := filepath.Clean(name)
	if strings.Contains(name, string(filepath.Separator)) ||
		strings.Contains(cleanFile, "..") ||
		filepath.IsAbs(cleanFile) ||
		cleanFile != name {
		return nil, fmt.Errorf("invalid file parameter: %s (must be a simple filename without path separators)", name)
	}

	// Ensure the destination directory exists and is writable
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
		name:     name,
		metaPath: filepath.Join(dir, "METADATA.json"),
	}

	if opt != nil {
		c.timestamp = opt.Timestamp
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
	return md.Name
}

// String returns a string representation of the client.
func (c *Client) String() string {
	return fmt.Sprintf("dir:%s, file:%s", c.dir, c.name)
}

// Upload uploads data from the reader to the file storage.
func (c *Client) Upload(ctx context.Context, reader io.Reader, id string) (retErr error) {
	filename := c.name
	if c.timestamp {
		if c.now == nil {
			c.now = func() time.Time {
				return time.Now().UTC()
			}
		}
		filename = timestampedPath(filename, c.now())
	}

	finalPath := filepath.Join(c.dir, filename)

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
	_, err = io.Copy(tmpFile, reader)
	if err != nil {
		return fmt.Errorf("failed to write to temporary file %s: %w", tmpPath, err)
	}
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file %s: %w", tmpPath, err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file %s: %w", tmpPath, err)
	}

	// Write metadata to temporary metadata file
	metadata := Metadata{
		ID:        id,
		Timestamp: time.Now().UnixMilli(),
		Name:      finalPath,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(tmpMetaPath, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write temporary metadata file %s: %w", tmpMetaPath, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename temporary file %s to %s: %w", tmpPath, finalPath, err)
	}

	if err := os.Rename(tmpMetaPath, c.metaPath); err != nil {
		os.Remove(finalPath)
		return fmt.Errorf("failed to rename temporary metadata file %s to %s: %w", tmpMetaPath, c.metaPath, err)
	}
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

// timestampedPath returns a new path with the given timestamp prepended.
// If path contains /, the timestamp is prepended to the last segment.
func timestampedPath(path string, t time.Time) string {
	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s_%s", t.Format("20060102150405"), parts[len(parts)-1])
	return strings.Join(parts, "/")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
