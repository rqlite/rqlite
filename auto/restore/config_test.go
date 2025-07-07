package restore

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/auto/aws"
	"github.com/rqlite/rqlite/v8/auto/gcp"
)

func Test_ReadConfigFile(t *testing.T) {
	t.Run("valid config file", func(t *testing.T) {
		// Create a temporary config file
		tempFile, err := os.CreateTemp("", "upload_config")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		content := []byte("key=value")
		if _, err := tempFile.Write(content); err != nil {
			t.Fatal(err)
		}
		tempFile.Close()

		data, err := ReadConfigFile(tempFile.Name())
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if !bytes.Equal(data, content) {
			t.Fatalf("Expected %v, got %v", content, data)
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := ReadConfigFile("nonexistentfile")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("file with environment variables", func(t *testing.T) {
		// Set an environment variable
		t.Setenv("TEST_VAR", "test_value")

		// Create a temporary config file with an environment variable
		tempFile, err := os.CreateTemp("", "upload_config")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		content := []byte("key=$TEST_VAR")
		if _, err := tempFile.Write(content); err != nil {
			t.Fatal(err)
		}
		tempFile.Close()

		data, err := ReadConfigFile(tempFile.Name())
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		expectedContent := []byte("key=test_value")
		if !bytes.Equal(data, expectedContent) {
			t.Fatalf("Expected %v, got %v", expectedContent, data)
		}
	})

	t.Run("longer file with environment variables", func(t *testing.T) {
		// Set an environment variable
		t.Setenv("TEST_VAR1", "test_value")

		// Create a temporary config file with an environment variable
		tempFile, err := os.CreateTemp("", "upload_config")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		content := []byte(`
key1=$TEST_VAR1
key2=TEST_VAR2`)
		if _, err := tempFile.Write(content); err != nil {
			t.Fatal(err)
		}
		tempFile.Close()

		data, err := ReadConfigFile(tempFile.Name())
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		expectedContent := []byte(`
key1=test_value
key2=TEST_VAR2`)
		if !bytes.Equal(data, expectedContent) {
			t.Fatalf("Expected %v, got %v", expectedContent, data)
		}
	})
}

func TestNewStorageClient(t *testing.T) {
	testCases := []struct {
		name        string
		input       []byte
		expectedCfg *Config
		expectS3    bool
		expectGCS   bool
		expectedErr error
	}{
		{
			name: "ValidS3Config",
			input: []byte(`
			{
				"version": 1,
				"type": "s3",
				"timeout": "30s",
				"sub": {
					"access_key_id": "test_id",
					"secret_access_key": "test_secret",
					"region": "us-west-2",
					"bucket": "test_bucket",
					"path": "test/path"
				}
			}
			`),
			expectedCfg: &Config{
				Version:           1,
				Type:              "s3",
				Timeout:           30 * auto.Duration(time.Second),
				ContinueOnFailure: false,
			},
			expectS3:    true,
			expectedErr: nil,
		},
		{
			name: "ValidS3ConfigNoOptionalFields",
			input: []byte(`
                        {
                                "version": 1,
                                "type": "s3",
								"continue_on_failure": true,
                                "sub": {
                                        "access_key_id": "test_id",
                                        "secret_access_key": "test_secret",
                                        "region": "us-west-2",
                                        "bucket": "test_bucket",
                                        "path": "test/path"
                                }
                        }
                        `),
			expectedCfg: &Config{
				Version:           1,
				Type:              "s3",
				Timeout:           auto.Duration(30 * time.Second),
				ContinueOnFailure: true,
			},
			expectS3:    true,
			expectedErr: nil,
		},
		{
			name: "InvalidVersion",
			input: []byte(`
			{
				"version": 2,
				"type": "s3",
				"timeout": "5m",
				"sub": {
					"access_key_id": "test_id",
					"secret_access_key": "test_secret",
					"region": "us-west-2",
					"bucket": "test_bucket",
					"path": "test/path"
				}
			}			`),
			expectedCfg: nil,
			expectedErr: auto.ErrInvalidVersion,
		},
		{
			name: "UnsupportedType",
			input: []byte(`
			{
				"version": 1,
				"type": "unsupported",
				"timeout": "24h",
				"sub": {
					"access_key_id": "test_id",
					"secret_access_key": "test_secret",
					"region": "us-west-2",
					"bucket": "test_bucket",
					"path": "test/path"
				}
			}			`),
			expectedCfg: nil,
			expectedErr: auto.ErrUnsupportedStorageType,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, sc, err := NewStorageClient(tc.input)
			if err != nil && !errors.Is(err, tc.expectedErr) {
				t.Fatalf("Test case %s failed, expected error %v, got %v", tc.name, tc.expectedErr, err)
			}

			if tc.expectS3 {
				if _, ok := sc.(*aws.S3Client); !ok {
					t.Fatalf("Test case %s failed, expected S3 client, got %T", tc.name, sc)
				}
			} else if tc.expectGCS {
				if _, ok := sc.(*gcp.GCSClient); !ok {
					t.Fatalf("Test case %s failed, expected GCS client, got %T", tc.name, sc)
				}
			} else {
				if sc != nil {
					t.Fatalf("Test case %s failed, expected no storage client, got %T", tc.name, sc)
				}
			}

			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("Test case %s failed, expected error %v, got %v", tc.name, tc.expectedErr, err)
			}

		})
	}
}

func compareConfig(a, b *Config) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Version == b.Version &&
		a.Type == b.Type &&
		a.Timeout == b.Timeout
}
