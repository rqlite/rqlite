package file

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
}

// Options represents options for the file storage client.
type Options struct {
	Timestamp bool
}

// Metadata represents metadata stored in the metadata file.
type Metadata struct {
	ID        string `json:"id"`
	Timestamp int64  `json:"timestamp,omitempty"`
	File      string `json:"file,omitempty"`
}

// NewClient creates a new file storage client.
func NewClient(dir, file string, opt *Options) (*Client, error) {
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
	return md.File
}

// String returns a string representation of the client.
func (c *Client) String() string {
	return fmt.Sprintf("dir:%s", c.dir)
}

// Upload uploads data from the reader to the file storage.
func (c *Client) Upload(ctx context.Context, reader io.Reader, id string) (retErr error) {
	finalPath := filepath.Join(c.dir, c.file)
	tmpPath := finalPath + ".tmp"
	tmpMetaPath := c.metaPath + ".tmp"
	defer func() {
		if retErr != nil {
			os.Remove(tmpMetaPath)
			os.Remove(tmpPath)
		} else {
			os.Rename(tmpPath, finalPath)
			os.Rename(tmpMetaPath, c.metaPath)
		}
	}()

	if err := os.Remove(c.metaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file %s: %w", c.metaPath, err)
	}

	fd, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temporary file %s: %w", tmpPath, err)
	}
	defer fd.Close()

	_, err = io.Copy(fd, reader)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %w", finalPath, err)
	}

	b, err := json.Marshal(Metadata{
		ID:        id,
		Timestamp: time.Now().UnixMilli(),
		File:      finalPath,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	return os.WriteFile(tmpMetaPath, b, 0644)
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
