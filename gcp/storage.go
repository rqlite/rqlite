package gcp

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var (
	GCSIDKey = "x-rqlite-auto-backup-id"
)

// GCSConfig is the subconfig for the GCS storage type
type GCSConfig struct {
	ProjectID       string `json:"project_id"`
	Bucket          string `json:"bucket"`
	Path            string `json:"path"`
	CredentialsFile string `json:"credentials_file,omitempty"`
	CredentialsJSON string `json:"credentials_json,omitempty"`
}

// GCSClient is a client for uploading data to GCS.
type GCSClient struct {
	projectID string
	bucket    string
	path      string
	timestamp bool

	client *storage.Client

	// These fields are used for testing via dependency injection.
	now func() time.Time
}

// GCSClientOpts are options for creating a GCSClient.
type GCSClientOpts struct {
	Timestamp bool
}

// NewGCSClient returns an instance of a GCSClient. opts can be nil.
func NewGCSClient(config *GCSConfig, opts *GCSClientOpts) (*GCSClient, error) {
	ctx := context.Background()

	var clientOpts []option.ClientOption

	// Set credentials if provided
	if config.CredentialsFile != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(config.CredentialsFile))
	} else if config.CredentialsJSON != "" {
		clientOpts = append(clientOpts, option.WithCredentialsJSON([]byte(config.CredentialsJSON)))
	}
	// If neither is provided, use default credentials

	client, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	gcsClient := &GCSClient{
		projectID: config.ProjectID,
		bucket:    config.Bucket,
		path:      config.Path,
		client:    client,
	}

	if opts != nil {
		gcsClient.timestamp = opts.Timestamp
	}

	return gcsClient, nil
}

// String returns a string representation of the GCSClient.
func (g *GCSClient) String() string {
	return fmt.Sprintf("gs://%s/%s", g.bucket, g.path)
}

// EnsureBucket ensures the bucket actually exists in GCS.
func (g *GCSClient) EnsureBucket(ctx context.Context) error {
	bucket := g.client.Bucket(g.bucket)
	if err := bucket.Create(ctx, g.projectID, nil); err != nil {
		return fmt.Errorf("failed to create bucket %s: %w", g.bucket, err)
	}
	return nil
}

// Upload uploads data to GCS.
func (g *GCSClient) Upload(ctx context.Context, reader io.Reader, id string) error {
	objectName := g.path
	if g.timestamp {
		if g.now == nil {
			g.now = func() time.Time {
				return time.Now().UTC()
			}
		}
		objectName = TimestampedPath(g.path, g.now())
	}

	bucket := g.client.Bucket(g.bucket)
	obj := bucket.Object(objectName)

	w := obj.NewWriter(ctx)
	defer w.Close()

	// Set metadata if ID is provided
	if id != "" {
		w.ObjectAttrs.Metadata = map[string]string{
			GCSIDKey: id,
		}
	}

	// Copy data from reader to writer
	if _, err := io.Copy(w, reader); err != nil {
		return fmt.Errorf("failed to upload to %s: %w", g.String(), err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close writer for %s: %w", g.String(), err)
	}

	return nil
}

// CurrentID returns the last ID uploaded to GCS.
func (g *GCSClient) CurrentID(ctx context.Context) (string, error) {
	bucket := g.client.Bucket(g.bucket)
	obj := bucket.Object(g.path)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get object attributes for %s: %w", g.String(), err)
	}

	id, ok := attrs.Metadata[GCSIDKey]
	if !ok {
		return "", fmt.Errorf("sum metadata not found for %s", g.String())
	}

	return id, nil
}

// Download downloads data from GCS.
func (g *GCSClient) Download(ctx context.Context, writer io.WriterAt) error {
	bucket := g.client.Bucket(g.bucket)
	obj := bucket.Object(g.path)

	r, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to create reader for %s: %w", g.String(), err)
	}
	defer r.Close()

	// Read all data and write to WriterAt
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read from %s: %w", g.String(), err)
	}

	if _, err := writer.WriteAt(data, 0); err != nil {
		return fmt.Errorf("failed to write data from %s: %w", g.String(), err)
	}

	return nil
}

// Delete deletes object from GCS.
func (g *GCSClient) Delete(ctx context.Context) error {
	bucket := g.client.Bucket(g.bucket)
	obj := bucket.Object(g.path)

	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete %s: %w", g.String(), err)
	}

	return nil
}

// TimestampedPath returns a new path with the given timestamp prepended.
// If path contains /, the timestamp is prepended to the last segment.
func TimestampedPath(path string, t time.Time) string {
	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s_%s", t.Format("20060102150405"), parts[len(parts)-1])
	return strings.Join(parts, "/")
}
