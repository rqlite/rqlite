package aws

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type uploader interface {
	UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type downloader interface {
	DownloadWithContext(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (n int64, err error)
}

// S3Client is a client for uploading data to S3.
type S3Client struct {
	region    string
	accessKey string
	secretKey string
	bucket    string
	key       string

	// These fields are used for testing via dependency injection.
	uploader   uploader
	downloader downloader
}

// NewS3Client returns an instance of an S3Client.
func NewS3Client(region, accessKey, secretKey, bucket, key string) *S3Client {
	return &S3Client{
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
	}
}

// String returns a string representation of the S3Client.
func (s *S3Client) String() string {
	return fmt.Sprintf("s3://%s/%s", s.bucket, s.key)
}

// Upload uploads data to S3.
func (s *S3Client) Upload(ctx context.Context, reader io.Reader) error {
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

	_, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to %v: %w", s, err)
	}

	return nil
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
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(s.region),
		Credentials: credentials.NewStaticCredentials(s.accessKey, s.secretKey, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 session: %w", err)
	}
	return sess, nil
}
