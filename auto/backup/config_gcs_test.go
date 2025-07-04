package backup

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/v8/auto"
	"github.com/rqlite/rqlite/v8/gcp"
)

func TestUnmarshalGCS(t *testing.T) {
	testCases := []struct {
		name         string
		input        []byte
		expectedCfg  *Config
		expectedGCS  *gcp.GCSConfig
		expectedErr  error
	}{
		{
			name: "ValidGCSConfig",
			input: []byte(`
			{
				"version": 1,
				"type": "gcs",
				"no_compress": false,
				"timestamp": true,
				"vacuum": false,
				"interval": "24h",
				"sub": {
					"project_id": "test-project",
					"bucket": "test-bucket",
					"path": "test/path",
					"credentials_file": "/path/to/creds.json"
				}
			}`),
			expectedCfg: &Config{
				Version:    1,
				Type:       "gcs",
				NoCompress: false,
				Timestamp:  true,
				Vacuum:     false,
				Interval:   24 * auto.Duration(time.Hour),
			},
			expectedGCS: &gcp.GCSConfig{
				ProjectID:       "test-project",
				Bucket:          "test-bucket",
				Path:            "test/path",
				CredentialsFile: "/path/to/creds.json",
			},
			expectedErr: nil,
		},
		{
			name: "ValidGCSConfigWithCredentialsJSON",
			input: []byte(`
			{
				"version": 1,
				"type": "gcs",
				"interval": "1h",
				"sub": {
					"project_id": "test-project",
					"bucket": "test-bucket",
					"path": "test/path",
					"credentials_json": "{\"type\": \"service_account\"}"
				}
			}`),
			expectedCfg: &Config{
				Version:    1,
				Type:       "gcs",
				NoCompress: false,
				Timestamp:  false,
				Vacuum:     false,
				Interval:   auto.Duration(time.Hour),
			},
			expectedGCS: &gcp.GCSConfig{
				ProjectID:       "test-project",
				Bucket:          "test-bucket",
				Path:            "test/path",
				CredentialsJSON: "{\"type\": \"service_account\"}",
			},
			expectedErr: nil,
		},
		{
			name: "InvalidVersion",
			input: []byte(`
			{
				"version": 2,
				"type": "gcs",
				"interval": "24h",
				"sub": {
					"project_id": "test-project",
					"bucket": "test-bucket",
					"path": "test/path"
				}
			}`),
			expectedCfg: nil,
			expectedGCS: nil,
			expectedErr: auto.ErrInvalidVersion,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, gcsCfg, err := UnmarshalGCS(tc.input)

			if tc.expectedErr != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tc.expectedErr)
				}
				if err.Error() != tc.expectedErr.Error() {
					t.Fatalf("Expected error %v, got %v", tc.expectedErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !compareConfig(cfg, tc.expectedCfg) {
				t.Fatalf("Expected config %+v, got %+v", tc.expectedCfg, cfg)
			}

			if !compareGCSConfig(gcsCfg, tc.expectedGCS) {
				t.Fatalf("Expected GCS config %+v, got %+v", tc.expectedGCS, gcsCfg)
			}
		})
	}
}

func compareGCSConfig(a, b *gcp.GCSConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.ProjectID == b.ProjectID &&
		a.Bucket == b.Bucket &&
		a.Path == b.Path &&
		a.CredentialsFile == b.CredentialsFile &&
		a.CredentialsJSON == b.CredentialsJSON
}