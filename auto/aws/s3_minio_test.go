package aws

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v8/internal/random"
)

func Test_S3Client_MinioUpload(t *testing.T) {
	t.Skip()
	bucketName := strings.ToLower(random.String())

	c, err := NewS3Client("http://localhost:9000", "region1", "minioadmin", "minioadmin", bucketName, "path", forcePathStyleOptions())
	if err != nil {
		t.Fatalf("error while creating aws S3 client: %v", err)
	}

	if err := c.EnsureBucket(context.Background()); err != nil {
		t.Fatalf("error while ensuring bucket: %v", err)
	}

	// Create a reader with some data
	data := "Hello, World!"
	reader := strings.NewReader(data)

	// Upload the data
	err = c.Upload(context.Background(), reader, "1234")
	if err != nil {
		t.Fatalf("error while uploading data: %v", err)
	}

	// Check the upload by downloaind the data and comparing it.
	f, err := os.CreateTemp("", "minio-download")
	if err != nil {
		t.Fatalf("error while creating temp file: %v", err)
	}
	defer f.Close()
	err = c.Download(context.Background(), f)
	if err != nil {
		t.Fatalf("error while downloading data: %v", err)
	}

	dataBytes, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("error while reading downloaded file: %v", err)
	}
	if string(dataBytes) != data {
		t.Fatalf("downloaded data does not match: %s", string(dataBytes))
	}
}
