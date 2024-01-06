package aws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func Test_NewS3Client(t *testing.T) {
	c := NewS3Client("endpoint1", "region1", "access", "secret", "bucket2", "key3", true)
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
	if c.forcePathStyle != true {
		t.Fatalf("expected forcePathStyle to be %v, got %v", true, c.forcePathStyle)
	}
}

func Test_S3Client_String(t *testing.T) {
	// Test native S3 with implicit endpoint
	c := NewS3Client("", "region1", "access", "secret", "bucket2", "key3", false)
	if c.String() != "s3://bucket2/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://bucket2/key3", c.String())
	}
	// Test native S3 with explicit endpoint
	c = NewS3Client("s3.amazonaws.com", "region1", "access", "secret", "bucket2", "key3", false)
	if c.String() != "s3://bucket2/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://bucket2/key3", c.String())
	}
	// Test non-native S3 (explicit endpoint) with non-path style (e.g. Wasabi)
	c = NewS3Client("s3.ca-central-1.wasabisys.com", "region1", "access", "secret", "bucket2", "key3", false)
	if c.String() != "s3://bucket2.s3.ca-central-1.wasabisys.com/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://bucket2.s3.ca-central-1.wasabisys.com/key3", c.String())
	}
	// Test non-native S3 (explicit endpoint) with forced path style (e.g. MinIO)
	c = NewS3Client("s3.minio.example.com", "region1", "access", "secret", "bucket2", "key3", true)
	if c.String() != "s3://s3.minio.example.com/bucket2/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://s3.minio.example.com/bucket2/key3", c.String())
	}
}

func TestS3ClientUploadOK(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"
	hash := "1234"
	expectedData := "test data"
	uploadedData := new(bytes.Buffer)

	mockUploader := &mockUploader{
		uploadFn: func(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			if *input.Bucket != bucket {
				t.Errorf("expected bucket to be %q, got %q", bucket, *input.Bucket)
			}
			if *input.Key != key {
				t.Errorf("expected key to be %q, got %q", key, *input.Key)
			}
			if *input.Metadata["rqlite_hash"] != hash {
				t.Errorf("expected hash to be %q, got %q", hash, *input.Metadata["rqlite_hash"])
			}
			if input.Body == nil {
				t.Errorf("expected body to be non-nil")
			}
			_, err := uploadedData.ReadFrom(input.Body)
			if err != nil {
				t.Errorf("error reading from input body: %v", err)
			}
			return &s3manager.UploadOutput{}, nil
		},
	}

	client := &S3Client{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
		uploader:  mockUploader,
	}

	reader := strings.NewReader("test data")
	err := client.Upload(context.Background(), reader, "1234")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if uploadedData.String() != expectedData {
		t.Errorf("expected uploaded data to be %q, got %q", expectedData, uploadedData.String())
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
	err := client.Upload(context.Background(), reader, "5678")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "some error related to S3") {
		t.Fatalf("Expected error to contain %q, got %q", "some error related to S3", err.Error())
	}
}

func TestS3ClientDownloadOK(t *testing.T) {
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"
	expectedData := "test data"

	mockDownloader := &mockDownloader{
		downloadFn: func(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (int64, error) {
			if *input.Bucket != bucket {
				t.Errorf("expected bucket to be %q, got %q", bucket, *input.Bucket)
			}
			if *input.Key != key {
				t.Errorf("expected key to be %q, got %q", key, *input.Key)
			}
			n, err := w.WriteAt([]byte(expectedData), 0)
			if err != nil {
				t.Errorf("error writing to writer: %v", err)
			}
			return int64(n), nil
		},
	}

	client := &S3Client{
		region:     region,
		accessKey:  accessKey,
		secretKey:  secretKey,
		bucket:     bucket,
		key:        key,
		downloader: mockDownloader,
	}

	writer := aws.NewWriteAtBuffer(make([]byte, len(expectedData)))
	err := client.Download(context.Background(), writer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(writer.Bytes()) != expectedData {
		t.Errorf("expected downloaded data to be %q, got %q", expectedData, writer.Bytes())
	}
}

func TestS3ClientDownloadFail(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	mockDownloader := &mockDownloader{
		downloadFn: func(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (n int64, err error) {
			return 0, fmt.Errorf("some error related to S3")
		},
	}

	client := &S3Client{
		endpoint:   endpoint,
		region:     region,
		accessKey:  accessKey,
		secretKey:  secretKey,
		bucket:     bucket,
		key:        key,
		downloader: mockDownloader,
	}

	writer := aws.NewWriteAtBuffer(nil)
	err := client.Download(context.Background(), writer)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "some error related to S3") {
		t.Fatalf("Expected error to contain %q, got %q", "some error related to S3", err.Error())
	}
}

type mockDownloader struct {
	downloadFn func(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (n int64, err error)
}

func (m *mockDownloader) DownloadWithContext(ctx aws.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*s3manager.Downloader)) (n int64, err error) {
	if m.downloadFn != nil {
		return m.downloadFn(ctx, w, input, opts...)
	}
	return 0, nil
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
