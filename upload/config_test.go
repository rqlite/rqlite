package upload

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestUnmarshal(t *testing.T) {
	testCases := []struct {
		name        string
		input       []byte
		expectedCfg *Config
		expectedS3  *S3Config
		expectedErr error
	}{
		{
			name: "ValidS3Config",
			input: []byte(`
			{
				"version": 1,
				"type": "s3",
				"compress": true,
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
				Version:  1,
				Type:     "s3",
				Compress: true,
				Interval: 24 * Duration(time.Hour),
			},
			expectedS3: &S3Config{
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
				"compress": true,
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
			expectedErr: ErrInvalidVersion,
		},
		{
			name: "UnsupportedType",
			input: []byte(`
			{
				"version": 1,
				"type": "unsupported",
				"compress": true,
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
			expectedErr: ErrUnsupportedStorageType,
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
		a.Compress == b.Compress &&
		a.Interval == b.Interval
}
