package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	// AWSS3IDKey is the S3 object metadata key under which the backup ID is stored.
	AWSS3IDKey = "x-rqlite-auto-backup-id"

	// ErrInvalidServerSideEncryption is returned when the server-side encryption
	// settings are not valid.
	ErrInvalidServerSideEncryption = errors.New("invalid server-side encryption settings")
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

	// ServerSideEncryption is the server-side encryption algorithm S3 applies
	// to uploaded objects. Valid values are "AES256", "aws:kms" and
	// "aws:kms:dsse". If empty, the bucket's default encryption applies.
	ServerSideEncryption string `json:"server_side_encryption,omitempty"`

	// SSEKMSKeyID is the KMS key S3 uses when ServerSideEncryption is a KMS
	// algorithm. If empty, S3 uses the AWS managed key for the account.
	SSEKMSKeyID string `json:"sse_kms_key_id,omitempty"`
}

// S3Client is a client for uploading data to S3.
type S3Client struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	bucket    string
	key       string
	timestamp bool

	sse         types.ServerSideEncryption
	sseKMSKeyID string

	s3 *s3.Client

	// These fields are used for testing via dependency injection.
	uploader   uploader
	downloader downloader
	now        func() time.Time
}

// S3ClientOpts are options for creating an S3Client.
type S3ClientOpts struct {
	ForcePathStyle bool
	Timestamp      bool

	// ServerSideEncryption and SSEKMSKeyID have the same meaning as the
	// fields of the same name in S3Config.
	ServerSideEncryption string
	SSEKMSKeyID          string
}

// NewS3Client returns an instance of an S3Client. opts can be nil.
func NewS3Client(endpoint, region, accessKey, secretKey, bucket, key string, opts *S3ClientOpts) (*S3Client, error) {
	sse, sseKMSKeyID, err := parseServerSideEncryption(opts)
	if err != nil {
		return nil, err
	}

	// Load the default config
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}

	// If credentials are provided, set them
	if accessKey != "" && secretKey != "" {
		cfg.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""))
	}

	// If an endpoint is provided, set it and the path style
	s3 := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if opts != nil {
			if endpoint != "" {
				if !hasProtocol(endpoint) {
					endpoint = fmt.Sprintf("https://%s", endpoint)
				}
				o.BaseEndpoint = aws.String(endpoint)
			}
			o.UsePathStyle = opts.ForcePathStyle
		}
	})

	client := &S3Client{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,

		sse:         sse,
		sseKMSKeyID: sseKMSKeyID,

		s3:         s3,
		uploader:   manager.NewUploader(s3),
		downloader: manager.NewDownloader(s3),
	}
	if opts != nil {
		client.timestamp = opts.Timestamp
	}
	return client, nil
}

// String returns a string representation of the S3Client.
func (s *S3Client) String() string {
	if s.endpoint == "" || isAWSEndpoint(s.endpoint) {
		// Native Amazon S3, use AWS's S3 URL format
		return fmt.Sprintf("s3://%s/%s", s.bucket, s.key)
	}
	return fmt.Sprintf("%s/%s/%s", s.endpoint, s.bucket, s.key)
}

// EnsureBucket ensures the bucket actually exists in S3.
func (s *S3Client) EnsureBucket(ctx context.Context) error {
	_, err := s.s3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket %v: %w", s.bucket, err)
	}
	return nil
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
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   reader,
	}

	if id != "" {
		input.Metadata = map[string]string{
			AWSS3IDKey: id,
		}
	}
	if s.sse != "" {
		input.ServerSideEncryption = s.sse
		if s.sseKMSKeyID != "" {
			input.SSEKMSKeyId = aws.String(s.sseKMSKeyID)
		}
	}
	_, err := s.uploader.Upload(ctx, input)
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

	result, err := s.s3.HeadObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get object head for %v: %w", s, err)
	}

	id, ok := result.Metadata[AWSS3IDKey]
	if !ok {
		return "", fmt.Errorf("ID metadata not found for %v", s)
	}
	return id, nil
}

// Download downloads data from S3.
func (s *S3Client) Download(ctx context.Context, writer io.WriterAt) error {
	_, err := s.downloader.Download(ctx, writer, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		return fmt.Errorf("failed to download from %v: %w", s, err)
	}
	return nil
}

// Delete deletes the object from S3.
func (s *S3Client) Delete(ctx context.Context) error {
	_, err := s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete %v: %w", s, err)
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

// parseServerSideEncryption validates the server-side encryption settings in
// opts and returns them in the form the S3 API expects. opts can be nil.
func parseServerSideEncryption(opts *S3ClientOpts) (types.ServerSideEncryption, string, error) {
	if opts == nil {
		return "", "", nil
	}
	sse := types.ServerSideEncryption(opts.ServerSideEncryption)
	switch sse {
	case "", types.ServerSideEncryptionAes256, types.ServerSideEncryptionAwsKms, types.ServerSideEncryptionAwsKmsDsse:
	default:
		return "", "", fmt.Errorf("%w: unknown algorithm %q", ErrInvalidServerSideEncryption, opts.ServerSideEncryption)
	}
	if opts.SSEKMSKeyID != "" && sse != types.ServerSideEncryptionAwsKms && sse != types.ServerSideEncryptionAwsKmsDsse {
		return "", "", fmt.Errorf("%w: a KMS key ID requires a KMS algorithm", ErrInvalidServerSideEncryption)
	}
	return sse, opts.SSEKMSKeyID, nil
}

func isAWSEndpoint(s string) bool {
	return strings.HasSuffix(s, "amazonaws.com")
}

func hasProtocol(s string) bool {
	return strings.Contains(s, "://")
}

type uploader interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

type downloader interface {
	Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (n int64, err error)
}
