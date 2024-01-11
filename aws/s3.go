package aws

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	AWSS3SumKey = "x-rqlite-sum"
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

	// These fields are used for testing via dependency injection.
	uploader   uploader
	downloader downloader
}

// NewS3Client returns an instance of an S3Client.
func NewS3Client(endpoint, region, accessKey, secretKey, bucket, key string, forcePathStyle bool) *S3Client {
	return &S3Client{
		endpoint:       endpoint,
		region:         region,
		accessKey:      accessKey,
		secretKey:      secretKey,
		bucket:         bucket,
		key:            key,
		forcePathStyle: forcePathStyle,
	}
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
func (s *S3Client) Upload(ctx context.Context, reader io.Reader, sum []byte) error {
	sess, err := s.createSession()
	if err != nil {
		return err
	}

	// If an uploader was not provided, use a real S3 uploader.
	var uploader uploader
	if s.uploader == nil {
		uploader = s3manager.NewUploader(sess)
	} else {
		uploader = s.uploader
	}

	input := &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
		Body:   reader,
	}

	if sum != nil {
		input.Metadata = map[string]*string{
			AWSS3SumKey: aws.String(fmt.Sprintf("%x", sum)),
		}
	}
	_, err = uploader.UploadWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload to %v: %w", s, err)
	}

	return nil
}

func (s *S3Client) LastSum(ctx context.Context) ([]byte, error) {
	sess, err := s.createSession()
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	}

	result, err := svc.HeadObjectWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object head for %v: %w", s, err)
	}

	sumHex, ok := result.Metadata[AWSS3SumKey]
	if !ok {
		return nil, fmt.Errorf("sum metadata not found for %v", s)
	}
	return hex.DecodeString(*sumHex)
}

// Download downloads data from S3.
func (s *S3Client) Download(ctx context.Context, writer io.WriterAt) error {
	sess, err := s.createSession()
	if err != nil {
		return err
	}

	// If a downloader was not provided, use a real S3 downloader.
	var downloader downloader
	if s.downloader == nil {
		downloader = s3manager.NewDownloader(sess)
	} else {
		downloader = s.downloader
	}

	_, err = downloader.DownloadWithContext(ctx, writer, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		return fmt.Errorf("failed to download from %v: %w", s, err)
	}

	return nil
}

func (s *S3Client) createSession() (*session.Session, error) {
	cfg := aws.Config{
		Endpoint:         aws.String(s.endpoint),
		Region:           aws.String(s.region),
		S3ForcePathStyle: aws.Bool(s.forcePathStyle),
	}
	// If credentials aren't provided by the user, the AWS SDK will use the default
	// credential provider chain, which supports environment variables, shared credentials
	// file, and EC2 instance roles.
	if s.accessKey != "" && s.secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentials(s.accessKey, s.secretKey, "")
	}
	sess, err := session.NewSession(&cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}
	return sess, nil
}

type uploader interface {
	UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type downloader interface {
	DownloadWithContext(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (n int64, err error)
}
