package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rqlite/rqlite/v8/random"
)

func Test_NewS3Client(t *testing.T) {
	c, err := NewS3Client("endpoint1", "region1", "access", "secret", "bucket2", "key3", forcePathStyleOptions())
	if err != nil {
		t.Fatalf("error while creating aws S3 client: %v", err)
	}
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
	if c.s3.Options().UsePathStyle != true {
		t.Fatalf("expected forcePathStyle to be %v, got %v", true, c.s3.Options().UsePathStyle)
	}
}

func Test_S3Client_String(t *testing.T) {
	// Test native S3 with implicit endpoint
	c, err := NewS3Client("", "region1", "access", "secret", "bucket2", "key3", noForcePathStyleOptions())
	if err != nil {
		t.Fatalf("error while creating aws S3 client: %v", err)
	}
	if c.String() != "s3://bucket2/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://bucket2/key3", c.String())
	}
	// Test native S3 with explicit endpoint
	c, err = NewS3Client("s3.amazonaws.com", "region1", "access", "secret", "bucket2", "key3", noForcePathStyleOptions())
	if err != nil {
		t.Fatalf("error while creating aws S3 client: %v", err)
	}
	if c.String() != "s3://bucket2/key3" {
		t.Fatalf("expected String() to be %q, got %q", "s3://bucket2/key3", c.String())
	}
	// Test non-native S3 (explicit endpoint) with non-path style (e.g. Wasabi)
	// without any protocol specified. This should default to https.
	c, err = NewS3Client("s3.ca-central-1.wasabisys.com", "region1", "access", "secret", "bucket2", "key3", noForcePathStyleOptions())
	if err != nil {
		t.Fatalf("error while creating aws S3 client: %v", err)
	}
	if exp, got := "https://s3.ca-central-1.wasabisys.com/bucket2/key3", c.String(); exp != got {
		t.Fatalf("expected String() to be %s, got %s", exp, got)
	}
	// Test non-native S3 (explicit endpoint) with non-path style (e.g. Wasabi)
	// with a protocol specified. This should use the specified protocol.
	c, err = NewS3Client("xyz://s3.ca-central-1.wasabisys.com", "region1", "access", "secret", "bucket2", "key3", noForcePathStyleOptions())
	if err != nil {
		t.Fatalf("error while creating aws S3 client: %v", err)
	}
	if exp, got := "xyz://s3.ca-central-1.wasabisys.com/bucket2/key3", c.String(); exp != got {
		t.Fatalf("expected String() to be %s, got %s", exp, got)
	}
}

func Test_S3ClientUploadOK(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"
	expectedData := "test data"
	uploadedData := new(bytes.Buffer)

	mockUploader := &mockUploader{
		uploadFn: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			if *input.Bucket != bucket {
				t.Errorf("expected bucket to be %q, got %q", bucket, *input.Bucket)
			}
			if *input.Key != key {
				t.Errorf("expected key to be %q, got %q", key, *input.Key)
			}
			if input.Body == nil {
				t.Errorf("expected body to be non-nil")
			}
			_, err := uploadedData.ReadFrom(input.Body)
			if err != nil {
				t.Errorf("error reading from input body: %v", err)
			}
			if input.Metadata == nil {
				t.Errorf("expected metadata to be non-nil")
			}
			exp, got := "some-id", input.Metadata[AWSS3IDKey]
			if exp != got {
				t.Errorf("expected metadata to contain %q, got %q", exp, got)
			}
			return &manager.UploadOutput{}, nil
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
	err := client.Upload(context.Background(), reader, "some-id")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if uploadedData.String() != expectedData {
		t.Errorf("expected uploaded data to be %q, got %q", expectedData, uploadedData.String())
	}
}

func Test_S3ClientUploadOK_Timestamped(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"
	timestampedKey := "your/key/20210701150405_path"
	expectedData := "test data"
	uploadedData := new(bytes.Buffer)
	mockUploader := &mockUploader{
		uploadFn: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			if *input.Bucket != bucket {
				t.Errorf("expected bucket to be %q, got %q", bucket, *input.Bucket)
			}
			if *input.Key != timestampedKey {
				t.Errorf("expected key to be %q, got %q", timestampedKey, *input.Key)
			}
			if input.Body == nil {
				t.Errorf("expected body to be non-nil")
			}
			_, err := uploadedData.ReadFrom(input.Body)
			if err != nil {
				t.Errorf("error reading from input body: %v", err)
			}
			if input.Metadata == nil {
				t.Errorf("expected metadata to be non-nil")
			}
			exp, got := "some-id", input.Metadata[AWSS3IDKey]
			if exp != got {
				t.Errorf("expected metadata to contain %q, got %q", exp, got)
			}
			return &manager.UploadOutput{}, nil
		},
	}

	client := &S3Client{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
		timestamp: true,
		uploader:  mockUploader,
		now: func() time.Time {
			return time.Date(2021, time.July, 1, 15, 4, 5, 0, time.UTC) // Controls timestampedKey
		},
	}

	reader := strings.NewReader("test data")
	err := client.Upload(context.Background(), reader, "some-id")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if uploadedData.String() != expectedData {
		t.Errorf("expected uploaded data to be %q, got %q", expectedData, uploadedData.String())
	}
}

// Test_S3ClientUploadOK_Timestamped_Changes tests that the key actually changes between uploads
// when timestamped uploads are enabled.
func Test_S3ClientUploadOK_Timestamped_Changes(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	timestampedKey1 := ""
	mockUploader := &mockUploader{
		uploadFn: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			if timestampedKey1 == "" {
				timestampedKey1 = *input.Key
			} else {
				if *input.Key == timestampedKey1 {
					t.Errorf("expected key for second upload to be different from %q, got %q", timestampedKey1, *input.Key)
				}
			}
			return &manager.UploadOutput{}, nil
		},
	}

	client := &S3Client{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
		timestamp: true,
		uploader:  mockUploader,
	}

	u := func(id string) {
		reader := strings.NewReader("test data")
		err := client.Upload(context.Background(), reader, id)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	u("some-id1")
	time.Sleep(time.Second)
	u("some-id2")
}

// Test_S3ClientUploadOK_Timestamped_NoChanges tests that the key does not change between
// uploads when timestamped uploads are disabled.
func Test_S3ClientUploadOK_Timestamped_NoChanges(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	timestampedKey1 := ""
	mockUploader := &mockUploader{
		uploadFn: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			if timestampedKey1 == "" {
				timestampedKey1 = *input.Key
			} else {
				if *input.Key != timestampedKey1 {
					t.Errorf("expected key for second upload to be same as %q, got %q", timestampedKey1, *input.Key)
				}
			}
			return &manager.UploadOutput{}, nil
		},
	}

	client := &S3Client{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		key:       key,
		timestamp: false,
		uploader:  mockUploader,
	}

	u := func(id string) {
		reader := strings.NewReader("test data")
		err := client.Upload(context.Background(), reader, id)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	u("some-id1")
	time.Sleep(time.Second)
	u("some-id2")
}

func Test_S3ClientUploadFail(t *testing.T) {
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	mockUploader := &mockUploader{
		uploadFn: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return &manager.UploadOutput{}, fmt.Errorf("some error related to S3")
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
	err := client.Upload(context.Background(), reader, "")
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "some error related to S3") {
		t.Fatalf("Expected error to contain %q, got %q", "some error related to S3", err.Error())
	}
}

func Test_S3ClientDownloadOK(t *testing.T) {
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"
	expectedData := "test data"

	mockDownloader := &mockDownloader{
		downloadFn: func(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (int64, error) {
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

	writer := manager.NewWriteAtBuffer(make([]byte, 0, len(expectedData)))
	err := client.Download(context.Background(), writer)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(writer.Bytes()) != expectedData {
		t.Errorf("expected downloaded data to be %q, got %q", expectedData, writer.Bytes())
	}
}

func Test_S3ClientDownloadFail(t *testing.T) {
	endpoint := "https://my-custom-s3-endpoint.com"
	region := "us-west-2"
	accessKey := "your-access-key"
	secretKey := "your-secret-key"
	bucket := "your-bucket"
	key := "your/key/path"

	mockDownloader := &mockDownloader{
		downloadFn: func(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (n int64, err error) {
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

	writer := manager.NewWriteAtBuffer(nil)
	err := client.Download(context.Background(), writer)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "some error related to S3") {
		t.Fatalf("Expected error to contain %q, got %q", "some error related to S3", err.Error())
	}
}

func Test_CreateDeleteOK(t *testing.T) {
	exists := func(client *s3.Client, bucket, key string) bool {
		t.Helper()
		input := &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}
		_, err := client.HeadObject(context.TODO(), input)
		if err != nil {
			var notFound *types.NotFound
			if errors.As(err, &notFound) {
				return false
			}
			t.Fatalf("error checking if object exists: %v", err)
		}
		return true
	}

	accesskey, ok := os.LookupEnv("RQLITE_S3_ACCESS_KEY")
	if !ok {
		t.Skip("RQLITE_S3_ACCESS_KEY is not set")
	}
	secretKey, ok := os.LookupEnv("RQLITE_S3_SECRET_ACCESS_KEY")
	if !ok {
		t.Skip("RQLITE_S3_SECRET_ACCESS_KEY is not set")
	}

	path := random.String()
	client, err := NewS3Client("s3.amazonaws.com", "us-west-2", accesskey, secretKey, "rqlite-testing-circleci", path, nil)
	if err != nil {
		t.Fatalf("error creating S3 client: %v", err)
	}
	if err := client.Upload(context.Background(), strings.NewReader("test data"), "the-id"); err != nil {
		t.Fatalf("error uploading to S3: %v", err)
	}
	if !exists(client.s3, client.bucket, client.key) {
		t.Fatalf("expected object to exist")
	}

	if err := client.Delete(context.Background()); err != nil {
		t.Fatalf("error deleting from S3: %v", err)
	}
	if exists(client.s3, client.bucket, client.key) {
		t.Fatalf("expected object to not exist")
	}
}

func Test_MetadataSet(t *testing.T) {
	accesskey, ok := os.LookupEnv("RQLITE_S3_ACCESS_KEY")
	if !ok {
		t.Skip("RQLITE_S3_ACCESS_KEY is not set")
	}
	secretKey, ok := os.LookupEnv("RQLITE_S3_SECRET_ACCESS_KEY")
	if !ok {
		t.Skip("RQLITE_S3_SECRET_ACCESS_KEY is not set")
	}

	path := random.String()
	client, err := NewS3Client("s3.amazonaws.com", "us-west-2", accesskey, secretKey, "rqlite-testing-circleci", path, nil)
	if err != nil {
		t.Fatalf("error creating S3 client: %v", err)
	}
	if err := client.Upload(context.Background(), strings.NewReader("test data"), "the-id"); err != nil {
		t.Fatalf("error uploading to S3: %v", err)
	}
	defer func() {
		client.Delete(context.Background())
	}()

	// Now check that the metadata is set
	id, err := client.CurrentID(context.Background())
	if err != nil {
		t.Fatalf("error getting current ID: %v", err)
	}
	if id != "the-id" {
		t.Fatalf("expected ID to be %q, got %q", "the-id", id)
	}
}

func Test_TimestampedPath(t *testing.T) {
	ts, err := time.Parse(time.RFC3339, "2021-07-01T15:04:05Z")
	if err != nil {
		t.Fatalf("error parsing time: %v", err)
	}

	if exp, got := "20210701150405_xxx", TimestampedPath("xxx", ts); exp != got {
		t.Fatalf("wrong timestamped path\nexp %s\ngot %s", exp, got)
	}
	if exp, got := "/20210701150405_xxx", TimestampedPath("/xxx", ts); exp != got {
		t.Fatalf("wrong timestamped path\nexp %s\ngot %s", exp, got)
	}
	if exp, got := "aaa/20210701150405_bbb", TimestampedPath("aaa/bbb", ts); exp != got {
		t.Fatalf("wrong timestamped path\nexp %s\ngot %s", exp, got)
	}
	if exp, got := "aaa/bbb/20210701150405_ccc", TimestampedPath("aaa/bbb/ccc", ts); exp != got {
		t.Fatalf("wrong timestamped path\nexp %s\ngot %s", exp, got)
	}
}

func Test_TimestampedPath_Changes(t *testing.T) {
	path1 := TimestampedPath("xxx", time.Now().UTC())
	time.Sleep(1 * time.Second)
	path2 := TimestampedPath("xxx", time.Now().UTC())
	if path1 == path2 {
		t.Fatalf("expected different paths, got %q", path1)
	}
}

type mockDownloader struct {
	downloadFn func(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (n int64, err error)
}

func (m *mockDownloader) Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, opts ...func(*manager.Downloader)) (n int64, err error) {
	if m.downloadFn != nil {
		return m.downloadFn(ctx, w, input, opts...)
	}
	return 0, nil
}

type mockUploader struct {
	uploadFn func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

func (m *mockUploader) Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	if m.uploadFn != nil {
		return m.uploadFn(ctx, input, opts...)
	}
	return &manager.UploadOutput{}, nil
}

func forcePathStyleOptions() *S3ClientOpts {
	return &S3ClientOpts{
		ForcePathStyle: true,
	}
}

func noForcePathStyleOptions() *S3ClientOpts {
	return &S3ClientOpts{
		ForcePathStyle: false,
	}
}
