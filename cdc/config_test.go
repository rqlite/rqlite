package cdc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_NewConfig_ValidURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"http URL", "http://example.com/cdc"},
		{"https URL", "https://example.com/cdc"},
		{"http with port", "http://localhost:9090/cdc"},
		{"https with port", "https://api.example.com:443/webhook"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewConfig(tt.url)
			if err != nil {
				t.Fatalf("NewConfig returned unexpected error for valid URL %q: %v", tt.url, err)
			}
			if config == nil {
				t.Fatal("NewConfig returned nil config for valid URL")
			}
			if config.Endpoint != tt.url {
				t.Fatalf("Expected endpoint %q, got %q", tt.url, config.Endpoint)
			}

			// Verify other fields have default values
			defaultConfig := DefaultConfig()
			if config.MaxBatchSz != defaultConfig.MaxBatchSz {
				t.Fatalf("Expected MaxBatchSz %d, got %d", defaultConfig.MaxBatchSz, config.MaxBatchSz)
			}
			if config.MaxBatchDelay != defaultConfig.MaxBatchDelay {
				t.Fatalf("Expected MaxBatchDelay %v, got %v", defaultConfig.MaxBatchDelay, config.MaxBatchDelay)
			}
		})
	}
}

func Test_NewConfig_InvalidURL_ValidFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	// Create a test config
	testConfig := &Config{
		LogOnly:                  true,
		Endpoint:                 "https://test.example.com/cdc",
		MaxBatchSz:               50,
		MaxBatchDelay:            5 * time.Second,
		HighWatermarkingDisabled: true,
		HighWatermarkInterval:    30 * time.Second,
		TransmitTimeout:          10 * time.Second,
		TransmitMaxRetries:       3,
		TransmitRetryPolicy:      ExponentialRetryPolicy,
		TransmitMinBackoff:       2 * time.Second,
		TransmitMaxBackoff:       2 * time.Minute,
	}

	// Marshall to JSON
	configData, err := json.Marshal(testConfig)
	if err != nil {
		t.Fatalf("Failed to marshal test config: %v", err)
	}

	// Write to temporary file
	configFile := filepath.Join(tempDir, "config.json")
	if err := os.WriteFile(configFile, configData, 0644); err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	// Test NewConfig with the file path
	config, err := NewConfig(configFile)
	if err != nil {
		t.Fatalf("NewConfig returned unexpected error for valid file path: %v", err)
	}
	if config == nil {
		t.Fatal("NewConfig returned nil config for valid file path")
	}

	// Verify all fields match
	if config.LogOnly != testConfig.LogOnly {
		t.Fatalf("Expected LogOnly %v, got %v", testConfig.LogOnly, config.LogOnly)
	}
	if config.Endpoint != testConfig.Endpoint {
		t.Fatalf("Expected Endpoint %q, got %q", testConfig.Endpoint, config.Endpoint)
	}
	if config.MaxBatchSz != testConfig.MaxBatchSz {
		t.Fatalf("Expected MaxBatchSz %d, got %d", testConfig.MaxBatchSz, config.MaxBatchSz)
	}
	if config.MaxBatchDelay != testConfig.MaxBatchDelay {
		t.Fatalf("Expected MaxBatchDelay %v, got %v", testConfig.MaxBatchDelay, config.MaxBatchDelay)
	}
	if config.HighWatermarkingDisabled != testConfig.HighWatermarkingDisabled {
		t.Fatalf("Expected HighWatermarkingDisabled %v, got %v", testConfig.HighWatermarkingDisabled, config.HighWatermarkingDisabled)
	}
	if config.HighWatermarkInterval != testConfig.HighWatermarkInterval {
		t.Fatalf("Expected HighWatermarkInterval %v, got %v", testConfig.HighWatermarkInterval, config.HighWatermarkInterval)
	}
	if config.TransmitTimeout != testConfig.TransmitTimeout {
		t.Fatalf("Expected TransmitTimeout %v, got %v", testConfig.TransmitTimeout, config.TransmitTimeout)
	}
	if config.TransmitMaxRetries != testConfig.TransmitMaxRetries {
		t.Fatalf("Expected TransmitMaxRetries %d, got %d", testConfig.TransmitMaxRetries, config.TransmitMaxRetries)
	}
	if config.TransmitRetryPolicy != testConfig.TransmitRetryPolicy {
		t.Fatalf("Expected TransmitRetryPolicy %d, got %d", testConfig.TransmitRetryPolicy, config.TransmitRetryPolicy)
	}
	if config.TransmitMinBackoff != testConfig.TransmitMinBackoff {
		t.Fatalf("Expected TransmitMinBackoff %v, got %v", testConfig.TransmitMinBackoff, config.TransmitMinBackoff)
	}
	if config.TransmitMaxBackoff != testConfig.TransmitMaxBackoff {
		t.Fatalf("Expected TransmitMaxBackoff %v, got %v", testConfig.TransmitMaxBackoff, config.TransmitMaxBackoff)
	}
}

func Test_NewConfig_NonExistentFile(t *testing.T) {
	nonExistentFile := "/tmp/this-file-does-not-exist.json"

	config, err := NewConfig(nonExistentFile)
	if err == nil {
		t.Fatal("Expected error for non-existent file, got nil")
	}
	if config != nil {
		t.Fatal("Expected nil config for non-existent file")
	}
}

func Test_NewConfig_InvalidJSON(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	// Write invalid JSON to temporary file
	invalidJSONFile := filepath.Join(tempDir, "invalid.json")
	invalidJSON := `{"endpoint": "test", "max_batch_size": "not_a_number"}`
	if err := os.WriteFile(invalidJSONFile, []byte(invalidJSON), 0644); err != nil {
		t.Fatalf("Failed to write invalid JSON file: %v", err)
	}

	config, err := NewConfig(invalidJSONFile)
	if err == nil {
		t.Fatal("Expected error for invalid JSON file, got nil")
	}
	if config != nil {
		t.Fatal("Expected nil config for invalid JSON file")
	}
}

func Test_NewConfig_EmptyString(t *testing.T) {
	config, err := NewConfig("")
	if err == nil {
		t.Fatal("Expected error for empty string, got nil")
	}
	if config != nil {
		t.Fatal("Expected nil config for empty string")
	}
}

func Test_NewConfig_PartialConfig(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	// Create a partial config with only some fields
	partialJSON := `{
		"endpoint": "https://partial.example.com/cdc",
		"max_batch_size": 25
	}`

	configFile := filepath.Join(tempDir, "partial.json")
	if err := os.WriteFile(configFile, []byte(partialJSON), 0644); err != nil {
		t.Fatalf("Failed to write partial config file: %v", err)
	}

	config, err := NewConfig(configFile)
	if err != nil {
		t.Fatalf("NewConfig returned unexpected error for partial config: %v", err)
	}
	if config == nil {
		t.Fatal("NewConfig returned nil config for partial config")
	}

	// Verify the specified fields
	if config.Endpoint != "https://partial.example.com/cdc" {
		t.Fatalf("Expected endpoint %q, got %q", "https://partial.example.com/cdc", config.Endpoint)
	}
	if config.MaxBatchSz != 25 {
		t.Fatalf("Expected MaxBatchSz %d, got %d", 25, config.MaxBatchSz)
	}

	// Verify unspecified fields have zero values
	if config.LogOnly != false {
		t.Fatalf("Expected LogOnly to be false (zero value), got %v", config.LogOnly)
	}
	if config.MaxBatchDelay != 0 {
		t.Fatalf("Expected MaxBatchDelay to be 0 (zero value), got %v", config.MaxBatchDelay)
	}
}
