package aws

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func Test_NewS3Client(t *testing.T) {
	c := NewS3Client("region1", "access", "secret", "bucket2", "key3")
	if c.region != "region1" {
		t.Fatalf("expected region to be %q, got %q", "region1", c.region)
	}
	if c.accessKey != "access" {
		t.Fatalf("expected accessKey to be %q, got %q", "access", c.accessKey)
	}
	if c.secretKey != "secret" {
		t.Fatalf("expected secretKey to be %q, got %q", "secret", c.secretKey)
	}
	if c.bucket != "bucket2" {
		t.Fatalf("expected bucket to be %q, got %q", "bucket2", c.bucket)
	}
	if c.key != "key3" {
		t.Fatalf("expected key to be %q, got %q", "key3", c.key)
	}
}

func Test_S3Client_String(t *testing.T) {
	c := NewS3Client("region1", "access", "secret", "bucket2", "key3")
	if c.String() != "s3://bucket2/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://bucket2/key3", c.String())
	}
}

func TestS3ClientUploadOK(t *testing.T) {
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	mockUploader := &mockUploader{
		uploadFn: func(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			if *input.Bucket != bucket {
				t.Errorf("expected bucket to be %q, got %q", bucket, *input.Bucket)
			}
			if *input.Key != key {
				t.Errorf("expected key to be %q, got %q", key, *input.Key)
			}
			if input.Body == nil {
				t.Errorf("expected body to be non-nil")
			}
			return &s3manager.UploadOutput{}, nil
		},
	}

	client := &S3Client{
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
		uploader:  mockUploader,
	}

	reader := strings.NewReader("test data")
	err := client.Upload(context.Background(), reader)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestS3ClientUploadFail(t *testing.T) {
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	mockUploader := &mockUploader{
		uploadFn: func(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			return &s3manager.UploadOutput{}, fmt.Errorf("some error related to S3")
		},
	}

	client := &S3Client{
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
		uploader:  mockUploader,
	}

	reader := strings.NewReader("test data")
	err := client.Upload(context.Background(), reader)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "some error related to S3") {
		t.Fatalf("Expected error to contain %q, got %q", "some error related to S3", err.Error())
	}
}

type mockUploader struct {
	uploadFn func(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

func (m *mockUploader) UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if m.uploadFn != nil {
		return m.uploadFn(ctx, input, opts...)
	}
	return &s3manager.UploadOutput{}, nil
}
