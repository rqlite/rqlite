package file

import (
	"context"
	"errors"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
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

	now        func() time.Time
	renameFile func(string, string) error
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
	fsutil.Remove(touchPath)

	c := &Client{
		dir:        dir,
		name:       name,
		metaPath:   filepath.Join(dir, "METADATA.json"),
		renameFile: fsutil.Rename,
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
			fsutil.Remove(tmpMetaPath)
			fsutil.Remove(tmpPath)
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

	// Keep the last committed payload while publishing the new one. Both
	// metadata and the stable-name payload require separate renames, so a
	// failed metadata rename must never delete the only good backup.
	rollbackPath := tmpPath + ".previous"
	havePrevious := false
	if info, err := os.Lstat(finalPath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("cannot replace non-regular backup file %s", finalPath)
		}
		// The hard link is in the same directory, so it is cheap even for
		// large SQLite backups. If unsupported, fail before replacing data.
		if err := os.Link(finalPath, rollbackPath); err != nil {
			return fmt.Errorf("failed to preserve previous backup %s: %w", finalPath, err)
		}
		havePrevious = true
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to inspect previous backup %s: %w", finalPath, err)
	}

	if err := c.renameFile(tmpPath, finalPath); err != nil {
		renameErr := fmt.Errorf("failed to rename temporary file %s to %s: %w", tmpPath, finalPath, err)
		if havePrevious {
			if cleanupErr := fsutil.Remove(rollbackPath); cleanupErr != nil {
				return errors.Join(renameErr, fmt.Errorf("failed to remove unused rollback link %s: %w", rollbackPath, cleanupErr))
			}
		}
		return renameErr
	}

	if err := c.renameFile(tmpMetaPath, c.metaPath); err != nil {
		metaErr := fmt.Errorf("failed to rename temporary metadata file %s to %s: %w", tmpMetaPath, c.metaPath, err)
		if havePrevious {
			// Restore the exact previous inode. If that fails, do not remove
			// rollbackPath: the previous good backup remains recoverable.
			if rollbackErr := c.renameFile(rollbackPath, finalPath); rollbackErr != nil {
				return errors.Join(metaErr, fmt.Errorf("failed to restore previous backup (preserved at %s): %w", rollbackPath, rollbackErr))
			}
		} else if cleanupErr := fsutil.Remove(finalPath); cleanupErr != nil {
			return errors.Join(metaErr, fmt.Errorf("failed to remove uncommitted backup %s: %w", finalPath, cleanupErr))
		}
		return metaErr
	}

	if havePrevious {
		if err := fsutil.Remove(rollbackPath); err != nil {
			// The new backup and metadata are already committed. Report the
			// stale previous version rather than silently leave extra data.
			return fmt.Errorf("backup committed, but failed to remove previous copy %s: %w", rollbackPath, err)
		}
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
