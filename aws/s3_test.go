package aws

import "testing"

func Test_NewS3Client(t *testing.T) {
	c := NewS3Client("region1", "access", "secret", "bucket2", "key3")
	if c.region != "region1" {
		t.Errorf("expected region to be %q, got %q", "region1", c.region)
	}
	if c.accessKey != "access" {
		t.Errorf("expected accessKey to be %q, got %q", "access", c.accessKey)
	}
	if c.secretKey != "secret" {
		t.Errorf("expected secretKey to be %q, got %q", "secret", c.secretKey)
	}
	if c.bucket != "bucket2" {
		t.Errorf("expected bucket to be %q, got %q", "bucket2", c.bucket)
	}
	if c.key != "key3" {
		t.Errorf("expected key to be %q, got %q", "key3", c.key)
	}
}

func Test_S3Client_String(t *testing.T) {
	c := NewS3Client("region1", "access", "secret", "bucket2", "key3")
	if c.String() != "s3://bucket2/key3" {
		t.Errorf("expected String() to be %q, got %q", "s3://bucket2/key3", c.String())
	}
}
