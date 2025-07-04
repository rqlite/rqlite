package main

import (
	"github.com/rqlite/rqlite/v8/auto/backup"
	"testing"
)

func TestStartAutoBackups_GCSConfig(t *testing.T) {
	gcsConfig := []byte(`{
		"version": 1,
		"type": "gcs",
		"interval": "1h",
		"timestamp": true,
		"sub": {
			"project_id": "test-project",
			"bucket": "test-bucket",
			"path": "test/path.db",
			"credentials_file": "/path/to/creds.json"
		}
	}`)

	// Test that we can parse GCS config without error
	cfg, gcsCfg, err := backup.UnmarshalGCS(gcsConfig)
	if err != nil {
		t.Fatalf("Failed to parse GCS config: %v", err)
	}

	if cfg.Type != "gcs" {
		t.Errorf("Expected type 'gcs', got '%s'", cfg.Type)
	}

	if gcsCfg.ProjectID != "test-project" {
		t.Errorf("Expected project_id 'test-project', got '%s'", gcsCfg.ProjectID)
	}

	if gcsCfg.Bucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got '%s'", gcsCfg.Bucket)
	}
}

func TestStartAutoBackups_S3ConfigStillWorks(t *testing.T) {
	s3Config := []byte(`{
		"version": 1,
		"type": "s3",
		"interval": "1h",
		"timestamp": true,
		"sub": {
			"region": "us-west-2",
			"access_key_id": "test-key",
			"secret_access_key": "test-secret",
			"bucket": "test-bucket",
			"path": "test/path.db"
		}
	}`)

	// Test that we can still parse S3 config without error
	cfg, s3Cfg, err := backup.Unmarshal(s3Config)
	if err != nil {
		t.Fatalf("Failed to parse S3 config: %v", err)
	}

	if cfg.Type != "s3" {
		t.Errorf("Expected type 's3', got '%s'", cfg.Type)
	}

	if s3Cfg.Region != "us-west-2" {
		t.Errorf("Expected region 'us-west-2', got '%s'", s3Cfg.Region)
	}

	if s3Cfg.Bucket != "test-bucket" {
		t.Errorf("Expected bucket 'test-bucket', got '%s'", s3Cfg.Bucket)
	}
}
