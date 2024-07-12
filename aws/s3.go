package aws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	AWSS3IDKey = http.CanonicalHeaderKey("x-rqlite-auto-backup-id")
)

// S3Config is the subconfig for the S3 storage type
type S3Config struct {
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Bucket          string `json:"bucket"`
	Path            string `json:"path"`
	ForcePathStyle  bool   `json:"force_path_style"`
}

// S3Client is a client for uploading data to S3.
type S3Client struct {
	endpoint       string
	region         string
	accessKey      string
	secretKey      string
	bucket         string
	key            string
	forcePathStyle bool
	timestamp      bool

	session *session.Session
	s3      *s3.S3

	// These fields are used for testing via dependency injection.
	uploader   uploader
	downloader downloader
	now        func() time.Time
}

// S3ClientOpts are options for creating an S3Client.
type S3ClientOpts struct {
	ForcePathStyle bool
	Timestamp      bool
}

// NewS3Client returns an instance of an S3Client. opts can be nil.
func NewS3Client(endpoint, region, accessKey, secretKey, bucket, key string, opts *S3ClientOpts) (*S3Client, error) {
	cfg := aws.Config{
		Endpoint: aws.String(endpoint),
		Region:   aws.String(region),
	}
	if opts != nil {
		cfg.S3ForcePathStyle = aws.Bool(opts.ForcePathStyle)
	}
	// If credentials aren't provided by the user, the AWS SDK will use the default
	// credential provider chain, which supports environment variables, shared credentials
	// file, and EC2 instance roles.
	if accessKey != "" && secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}
	sess, err := session.NewSession(&cfg)
	if err != nil {
		return nil, err
	}

	s3 := s3.New(sess)

	client := &S3Client{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,

		session: sess,
		s3:      s3,

		uploader:   s3manager.NewUploaderWithClient(s3),
		downloader: s3manager.NewDownloaderWithClient(s3),
	}
	if opts != nil {
		client.forcePathStyle = opts.ForcePathStyle
		client.timestamp = opts.Timestamp
	}
	return client, nil
}

// String returns a string representation of the S3Client.
func (s *S3Client) String() string {
	if s.endpoint == "" || strings.HasSuffix(s.endpoint, "amazonaws.com") {
		// Native Amazon S3, use AWS's S3 URL format
		return fmt.Sprintf("s3://%s/%s", s.bucket, s.key)
	} else if !s.forcePathStyle {
		// Endpoint specified but not using path style (e.g. Wasabi)
		return fmt.Sprintf("s3://%s.%s/%s", s.bucket, s.endpoint, s.key)
	} else {
		// Endpoint specified and using path style (e.g. MinIO)
		return fmt.Sprintf("s3://%s/%s/%s", s.endpoint, s.bucket, s.key)
	}
}

// Upload uploads data to S3.
func (s *S3Client) Upload(ctx context.Context, reader io.Reader, id string) error {
	key := s.key
	if s.timestamp {
		if s.now == nil {
			s.now = func() time.Time {
				return time.Now().UTC()
			}
		}
		key = TimestampedPath(key, s.now())
	}
	input := &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	if id != "" {
		input.Metadata = map[string]*string{
			AWSS3IDKey: aws.String(id),
		}
	}
	_, err := s.uploader.UploadWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to %v: %w", s, err)
	}

	return nil
}

// CurrentID returns the last ID uploaded to S3.
func (s *S3Client) CurrentID(ctx context.Context) (string, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	}

	result, err := s.s3.HeadObjectWithContext(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get object head for %v: %w", s, err)
	}

	id, ok := result.Metadata[AWSS3IDKey]
	if !ok {
		return "", fmt.Errorf("sum metadata not found for %v", s)
	}
	return *id, nil
}

// Download downloads data from S3.
func (s *S3Client) Download(ctx context.Context, writer io.WriterAt) error {
	_, err := s.downloader.DownloadWithContext(ctx, writer, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		return fmt.Errorf("failed to download from %v: %w", s, err)
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

type uploader interface {
	UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type downloader interface {
	DownloadWithContext(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (n int64, err error)
}
