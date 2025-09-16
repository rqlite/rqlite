package file

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Config represents configuration for the file storage client.
type Config struct {
	Dir  string `json:"dir"`
	File string `json:"file"`
}

// Client represents a file storage client.
type Client struct {
	path   string
	idPath string
}

// Options represents options for the file storage client.
type Options struct {
	Timestamp bool
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

	return &Client{
		path:   filepath.Join(dir, file),
		idPath: filepath.Join(dir, "METADATA"),
	}, nil
}

// Upload uploads data from the reader to the file storage.
func (c *Client) Upload(ctx context.Context, reader io.Reader, id string) (retErr error) {
	tmpPath := c.path + ".tmp"
	tmpIDPath := c.idPath + ".tmp"
	defer func() {
		if retErr != nil {
			os.Remove(tmpIDPath)
			os.Remove(tmpPath)
		} else {
			os.Rename(tmpPath, c.path)
			os.Rename(tmpIDPath, c.idPath)
		}
	}()

	if err := os.Remove(c.idPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file %s: %w", c.idPath, err)
	}

	fd, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temporary file %s: %w", tmpPath, err)
	}
	defer fd.Close()

	_, err = io.Copy(fd, reader)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %w", c.path, err)
	}

	if err := os.WriteFile(tmpIDPath, []byte(id), 0644); err != nil {
		return fmt.Errorf("failed to write to file %s: %w", tmpIDPath, err)
	}
	return nil
}

// CurrentID returns the current ID stored in the metadata file.
func (c *Client) CurrentID(ctx context.Context) (string, bool, error) {
	data, err := os.ReadFile(c.idPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to read file %s: %w", c.idPath, err)
	}
	return string(data), true, nil
}

// String returns a string representation of the client.
func (c *Client) String() string {
	return fmt.Sprintf("file:%s", c.path)
}
