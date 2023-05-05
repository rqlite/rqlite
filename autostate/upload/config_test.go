package upload

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/rqlite/rqlite/autostate"
	"github.com/rqlite/rqlite/aws"
)

func Test_ReadConfigFile(t *testing.T) {
	t.Run("valid config file", func(t *testing.T) {
		// Create a temporary config file
		tempFile, err := ioutil.TempFile("", "upload_config")
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

	t.Run("non-existent file", func(t *testing.T) {
		_, err := ReadConfigFile("nonexistentfile")
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("file with environment variables", func(t *testing.T) {
		// Set an environment variable
		if err := os.Setenv("TEST_VAR", "test_value"); err != nil {
			t.Fatal(err)
		}
		defer os.Unsetenv("TEST_VAR")

		// Create a temporary config file with an environment variable
		tempFile, err := ioutil.TempFile("", "upload_config")
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
		if err := os.Setenv("TEST_VAR1", "test_value"); err != nil {
			t.Fatal(err)
		}
		defer os.Unsetenv("TEST_VAR1")

		// Create a temporary config file with an environment variable
		tempFile, err := ioutil.TempFile("", "upload_config")
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

func TestUnmarshal(t *testing.T) {
	testCases := []struct {
		name        string
		input       []byte
		expectedCfg *Config
		expectedS3  *aws.S3Config
		expectedErr error
	}{
		{
			name: "ValidS3Config",
			input: []byte(`
			{
				"version": 1,
				"type": "s3",
				"no_compress": true,
				"interval": "24h",
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
				NoCompress: true,
				Interval:   24 * autostate.Duration(time.Hour),
			},
			expectedS3: &aws.S3Config{
				AccessKeyID:     "test_id",
				SecretAccessKey: "test_secret",
				Region:          "us-west-2",
				Bucket:          "test_bucket",
				Path:            "test/path",
			},
			expectedErr: nil,
		},
		{
			name: "ValidS3ConfigNoptionalFields",
			input: []byte(`
                        {
                                "version": 1,
                                "type": "s3",
                                "interval": "24h",
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
				Interval:   24 * autostate.Duration(time.Hour),
			},
			expectedS3: &aws.S3Config{
				AccessKeyID:     "test_id",
				SecretAccessKey: "test_secret",
				Region:          "us-west-2",
				Bucket:          "test_bucket",
				Path:            "test/path",
			},
			expectedErr: nil,
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
			expectedS3:  nil,
			expectedErr: autostate.ErrInvalidVersion,
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
			expectedS3:  nil,
			expectedErr: autostate.ErrUnsupportedStorageType,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, s3Cfg, err := Unmarshal(tc.input)
			_ = s3Cfg

			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("Test case %s failed, expected error %v, got %v", tc.name, tc.expectedErr, err)
			}

			if !compareConfig(cfg, tc.expectedCfg) {
				t.Fatalf("Test case %s failed, expected config %+v, got %+v", tc.name, tc.expectedCfg, cfg)
			}

			if tc.expectedS3 != nil {
				if !reflect.DeepEqual(s3Cfg, tc.expectedS3) {
					t.Fatalf("Test case %s failed, expected S3Config %+v, got %+v", tc.name, tc.expectedS3, s3Cfg)
				}
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
