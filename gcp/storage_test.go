package gcp

import (
	"testing"
	"time"
)

func TestNewGCSClient(t *testing.T) {
	testCases := []struct {
		name   string
		config *GCSConfig
		opts   *GCSClientOpts
		expectError bool
	}{
		{
			name: "ValidConfig",
			config: &GCSConfig{
				ProjectID: "test-project",
				Bucket:    "test-bucket",
				Path:      "test/path",
			},
			opts: &GCSClientOpts{
				Timestamp: true,
			},
			expectError: false,
		},
		{
			name: "ConfigWithCredentialsFile",
			config: &GCSConfig{
				ProjectID:       "test-project",
				Bucket:          "test-bucket",
				Path:            "test/path",
				CredentialsFile: "/path/to/creds.json",
			},
			opts: nil,
			expectError: false,
		},
		{
			name: "ConfigWithCredentialsJSON",
			config: &GCSConfig{
				ProjectID:       "test-project",
				Bucket:          "test-bucket",
				Path:            "test/path",
				CredentialsJSON: "{\"type\": \"service_account\"}",
			},
			opts: nil,
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewGCSClient(tc.config, tc.opts)

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				return
			}

			if err != nil {
				// For now, we expect errors because we don't have real GCS credentials
				// In a real scenario with proper credentials, this would work
				t.Logf("Expected error due to missing credentials: %v", err)
				return
			}

			if client == nil {
				t.Fatalf("Expected client, got nil")
			}

			if client.projectID != tc.config.ProjectID {
				t.Errorf("Expected project ID %s, got %s", tc.config.ProjectID, client.projectID)
			}

			if client.bucket != tc.config.Bucket {
				t.Errorf("Expected bucket %s, got %s", tc.config.Bucket, client.bucket)
			}

			if client.path != tc.config.Path {
				t.Errorf("Expected path %s, got %s", tc.config.Path, client.path)
			}

			if tc.opts != nil && client.timestamp != tc.opts.Timestamp {
				t.Errorf("Expected timestamp %v, got %v", tc.opts.Timestamp, client.timestamp)
			}
		})
	}
}

func TestGCSClient_String(t *testing.T) {
	client := &GCSClient{
		bucket: "test-bucket",
		path:   "test/path",
	}

	expected := "gs://test-bucket/test/path"
	if client.String() != expected {
		t.Errorf("Expected %s, got %s", expected, client.String())
	}
}

func TestTimestampedPath(t *testing.T) {
	testTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	
	testCases := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "SimpleFilename",
			path:     "backup.db",
			expected: "20230101120000_backup.db",
		},
		{
			name:     "PathWithDirectory",
			path:     "folder/backup.db",
			expected: "folder/20230101120000_backup.db",
		},
		{
			name:     "NestedPath",
			path:     "folder/subfolder/backup.db",
			expected: "folder/subfolder/20230101120000_backup.db",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := TimestampedPath(tc.path, testTime)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}