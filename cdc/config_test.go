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
		{"http with BasicAuth", "http://user:pass@example.com/cdc"},
		{"https with BasicAuth", "https://username:password@api.example.com:443/webhook"},
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
			defConfig := DefaultConfig()
			if got, exp := config.MaxBatchSz, defConfig.MaxBatchSz; got != exp {
				t.Fatalf("Expected MaxBatchSz %d, got %d", exp, got)
			}
			if config.MaxBatchDelay != 0 {
				t.Fatalf("Expected MaxBatchDelay %v, got %v", 0, config.MaxBatchDelay)
			}
		})
	}
}

func Test_NewConfig_InvalidURL_ValidFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	// Create a test config
	testConfig := &Config{
		LogOnly:               true,
		Endpoint:              "https://test.example.com/cdc",
		MaxBatchSz:            50,
		MaxBatchDelay:         5 * time.Second,
		HighWatermarkInterval: 30 * time.Second,
		TransmitTimeout:       10 * time.Second,
		TransmitMaxRetries:    intPtr(3),
		TransmitRetryPolicy:   ExponentialRetryPolicy,
		TransmitMinBackoff:    2 * time.Second,
		TransmitMaxBackoff:    2 * time.Minute,
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
	if config.HighWatermarkInterval != testConfig.HighWatermarkInterval {
		t.Fatalf("Expected HighWatermarkInterval %v, got %v", testConfig.HighWatermarkInterval, config.HighWatermarkInterval)
	}
	if config.TransmitTimeout != testConfig.TransmitTimeout {
		t.Fatalf("Expected TransmitTimeout %v, got %v", testConfig.TransmitTimeout, config.TransmitTimeout)
	}
	if *config.TransmitMaxRetries != *testConfig.TransmitMaxRetries {
		t.Fatalf("Expected TransmitMaxRetries %d, got %d", *testConfig.TransmitMaxRetries, *config.TransmitMaxRetries)
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

func TestConfig_TLSConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantNil bool
		wantErr bool
	}{
		{
			name:    "no TLS config",
			config:  &Config{},
			wantNil: true,
			wantErr: false,
		},
		{
			name: "insecure skip verify only",
			config: &Config{
				TLS: &TLSConfiguration{
					InsecureSkipVerify: true,
				},
			},
			wantNil: false,
			wantErr: false,
		},
		{
			name: "server name only",
			config: &Config{
				TLS: &TLSConfiguration{
					ServerName: "example.com",
				},
			},
			wantNil: false,
			wantErr: false,
		},
		{
			name: "invalid cert/key combination",
			config: &Config{
				TLS: &TLSConfiguration{
					CertFile: "nonexistent.crt",
					KeyFile:  "nonexistent.key"},
			},
			wantNil: true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsConfig, err := tt.config.TLSConfig()

			if tt.wantErr && err == nil {
				t.Error("TLSConfig() expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("TLSConfig() unexpected error: %v", err)
			}
			if tt.wantNil && tlsConfig != nil {
				t.Error("TLSConfig() expected nil, got non-nil config")
			}
			if !tt.wantNil && !tt.wantErr && tlsConfig == nil {
				t.Error("TLSConfig() expected non-nil config, got nil")
			}
		})
	}
}

func TestNewConfig_WithTLSFields(t *testing.T) {
	// Create a temporary JSON config file with TLS fields
	tmpFile, err := os.CreateTemp("", "cdc_tls_config_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	configJSON := `{
		"endpoint": "https://secure.example.com/cdc",
		"max_batch_size": 25,
		"tls": {
		    "insecure_skip_verify": true,
		    "server_name": "secure.example.com"
	}
	}`

	if _, err := tmpFile.WriteString(configJSON); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	config, err := NewConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("NewConfig() failed: %v", err)
	}

	if config.Endpoint != "https://secure.example.com/cdc" {
		t.Errorf("Expected endpoint 'https://secure.example.com/cdc', got '%s'", config.Endpoint)
	}
	if config.MaxBatchSz != 25 {
		t.Errorf("Expected MaxBatchSz 25, got %d", config.MaxBatchSz)
	}
	if !config.TLS.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify to be true")
	}
	if got, exp := config.TLS.ServerName, "secure.example.com"; got != exp {
		t.Errorf("Expected ServerName '%s', got '%s'", exp, got)
	}

	// Test that TLSConfig works with these fields
	tlsConfig, err := config.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig() failed: %v", err)
	}
	if tlsConfig == nil {
		t.Error("Expected non-nil TLS config")
	}
	if !tlsConfig.InsecureSkipVerify {
		t.Error("Expected TLS config InsecureSkipVerify to be true")
	}
	if got, exp := tlsConfig.ServerName, "secure.example.com"; got != exp {
		t.Errorf("Expected TLS config ServerName '%s', got '%s'", exp, got)
	}
}
