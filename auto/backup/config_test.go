package backup

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/aws"
	"github.com/rqlite/rqlite/v8/gcp"
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

func Test_NewStorageClient(t *testing.T) {
	gcsCredsFile := createGCSCredFile(t)
	defer os.Remove(gcsCredsFile)

	testCases := []struct {
		name           string
		input          []byte
		expectedCfg    *Config
		expectedClient StorageClient
		expectedErr    error
	}{
		{
			name: "ValidS3Config",
			input: []byte(`
			{
				"version": 1,
				"type": "s3",
				"no_compress": true,
				"timestamp": true,
				"interval": "24h",
				"sub": {
					"access_key_id": "test_id",
					"secret_access_key": "test_secret",
					"region": "us-west-2",
					"vacuum": true,
					"bucket": "test_bucket",
					"path": "test/path"
				}
			}
			`),
			expectedCfg: &Config{
				Version:    1,
				Type:       "s3",
				NoCompress: true,
				Timestamp:  true,
				Vacuum:     true,
				Interval:   24 * auto.Duration(time.Hour),
			},
			expectedClient: mustNewS3Client(t, "", "us-west-2", "test_id", "test_secret", "test_bucket", "test/path"),
			expectedErr:    nil,
		},
		{
			name: "ValidS3ConfigNoptionalFields",
			input: []byte(`
                        {
                                "version": 1,
                                "type": "s3",
                                "interval": "24h",
                                "vacuum": false,
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
				Version:    1,
				Type:       "s3",
				NoCompress: false,
				Timestamp:  false,
				Interval:   24 * auto.Duration(time.Hour),
				Vacuum:     false,
			},
			expectedClient: mustNewS3Client(t, "", "us-west-2", "test_id", "test_secret", "test_bucket", "test/path"),
			expectedErr:    nil,
		},
		{
			name: "ValidGCSConfig",
			input: []byte(`
				{
					"version": 1,
					"type": "gcs",
					"no_compress": true,
					"timestamp": true,
					"interval": "24h",
					"sub": {
						"bucket": "test_bucket",
						"name": "test/path",
						"project_id": "test_project",
						"credentials_file": "` + gcsCredsFile + `"
					}
				}
				`),
			expectedCfg: &Config{
				Version:    1,
				Type:       "s3",
				NoCompress: true,
				Timestamp:  true,
				Vacuum:     true,
				Interval:   24 * auto.Duration(time.Hour),
			},
			expectedClient: mustNewGCSClient(t, "test_bucket", "test/path", "test_project", gcsCredsFile),
			expectedErr:    nil,
		},
		{
			name: "InvalidVersion",
			input: []byte(`
			{
				"version": 2,
				"type": "s3",
				"no_compress": false,
				"interval": "24h",
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
				"no_compress": true,
				"interval": "24h",
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
			cfg, sc, err := NewStorageClient(tc.input)
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("Test case %s failed, expected error %v, got %v", tc.name, tc.expectedErr, err)
			}

			if tc.expectedClient != nil {
				switch tc.expectedClient.(type) {
				case *aws.S3Client:
					_, ok := sc.(*aws.S3Client)
					if !ok {
						t.Fatalf("Test case %s failed, expected S3Client, got %T", tc.name, sc)
					}
				case *gcp.GCSClient:
					_, ok := sc.(*gcp.GCSClient)
					if !ok {
						t.Fatalf("Test case %s failed, expected GCSClient, got %T", tc.name, sc)
					}
				default:
					t.Fatalf("Test case %s failed, unexpected client type %T", tc.name, sc)
				}
			}

			if !compareConfig(cfg, tc.expectedCfg) {
				t.Fatalf("Test case %s failed, expected config %+v, got %+v", tc.name, tc.expectedCfg, cfg)
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
		a.NoCompress == b.NoCompress &&
		a.Interval == b.Interval
}

func mustNewS3Client(t *testing.T, endpoint, region, accessKey, secretKey, bucket, key string) *aws.S3Client {
	t.Helper()
	client, err := aws.NewS3Client(endpoint, region, accessKey, secretKey, bucket, key, nil)
	if err != nil {
		t.Fatalf("Failed to create S3 client: %v", err)
	}
	return client
}

func mustNewGCSClient(t *testing.T, bucket, name, projectID, credentialsFile string) *gcp.GCSClient {
	t.Helper()
	client, err := gcp.NewGCSClient(&gcp.GCSConfig{
		Bucket:          bucket,
		Name:            name,
		ProjectID:       projectID,
		CredentialsPath: credentialsFile,
	})
	if err != nil {
		t.Fatalf("Failed to create GCS client: %v", err)
	}
	return client
}

func createGCSCredFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "cred-*.json")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	cred := `{"client_email":"test@example.com","private_key":"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----"}`
	if _, err = f.WriteString(cred); err != nil {
		t.Fatalf("write cred: %v", err)
	}
	f.Close()
	return f.Name()
}
