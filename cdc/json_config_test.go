package cdc

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestParseJSONConfig(t *testing.T) {
	testCases := []struct {
		name        string
		jsonData    string
		expectError bool
	}{
		{
			name: "Complete valid config",
			jsonData: `{
				"endpoint": "https://webhook.example.com/cdc",
				"log_only": true,
				"max_batch_size": 50,
				"max_batch_delay": "2s",
				"high_watermarking_disabled": false,
				"high_watermark_interval": "5s",
				"transmit_timeout": "10s",
				"transmit_max_retries": 3,
				"transmit_retry_policy": "exponential",
				"transmit_min_backoff": "500ms",
				"transmit_max_backoff": "2m"
			}`,
			expectError: false,
		},
		{
			name: "Minimal config",
			jsonData: `{
				"endpoint": "http://localhost:9000/cdc"
			}`,
			expectError: false,
		},
		{
			name:        "Empty config",
			jsonData:    `{}`,
			expectError: false,
		},
		{
			name: "Invalid duration",
			jsonData: `{
				"max_batch_delay": "invalid_duration"
			}`,
			expectError: true,
		},
		{
			name: "Invalid retry policy",
			jsonData: `{
				"transmit_retry_policy": "invalid_policy"
			}`,
			expectError: true,
		},
		{
			name: "Invalid JSON",
			jsonData: `{
				"endpoint": "https://webhook.example.com/cdc"
				"log_only": true
			}`,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseJSONConfig([]byte(tc.jsonData))

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if cfg == nil {
					t.Errorf("Expected config but got nil")
				}
			}
		})
	}
}

func TestParseJSONConfigValues(t *testing.T) {
	jsonData := `{
		"endpoint": "https://webhook.example.com/cdc",
		"log_only": true,
		"max_batch_size": 50,
		"max_batch_delay": "2s",
		"high_watermarking_disabled": true,
		"high_watermark_interval": "5s",
		"transmit_timeout": "10s",
		"transmit_max_retries": 3,
		"transmit_retry_policy": "exponential",
		"transmit_min_backoff": "500ms",
		"transmit_max_backoff": "2m"
	}`

	cfg, err := ParseJSONConfig([]byte(jsonData))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if cfg.Endpoint != "https://webhook.example.com/cdc" {
		t.Errorf("Expected endpoint 'https://webhook.example.com/cdc', got %s", cfg.Endpoint)
	}

	if !cfg.LogOnly {
		t.Errorf("Expected LogOnly to be true")
	}

	if cfg.MaxBatchSz != 50 {
		t.Errorf("Expected MaxBatchSz 50, got %d", cfg.MaxBatchSz)
	}

	if cfg.MaxBatchDelay != 2*time.Second {
		t.Errorf("Expected MaxBatchDelay 2s, got %v", cfg.MaxBatchDelay)
	}

	if !cfg.HighWatermarkingDisabled {
		t.Errorf("Expected HighWatermarkingDisabled to be true")
	}

	if cfg.HighWatermarkInterval != 5*time.Second {
		t.Errorf("Expected HighWatermarkInterval 5s, got %v", cfg.HighWatermarkInterval)
	}

	if cfg.TransmitTimeout != 10*time.Second {
		t.Errorf("Expected TransmitTimeout 10s, got %v", cfg.TransmitTimeout)
	}

	if cfg.TransmitMaxRetries != 3 {
		t.Errorf("Expected TransmitMaxRetries 3, got %d", cfg.TransmitMaxRetries)
	}

	if cfg.TransmitRetryPolicy != ExponentialRetryPolicy {
		t.Errorf("Expected ExponentialRetryPolicy, got %d", cfg.TransmitRetryPolicy)
	}

	if cfg.TransmitMinBackoff != 500*time.Millisecond {
		t.Errorf("Expected TransmitMinBackoff 500ms, got %v", cfg.TransmitMinBackoff)
	}

	if cfg.TransmitMaxBackoff != 2*time.Minute {
		t.Errorf("Expected TransmitMaxBackoff 2m, got %v", cfg.TransmitMaxBackoff)
	}
}

func TestReadConfigFile(t *testing.T) {
	t.Run("file with environment variables", func(t *testing.T) {
		// Set an environment variable
		t.Setenv("TEST_ENDPOINT", "https://test.example.com/cdc")

		// Create a temporary config file with an environment variable
		tempFile, err := os.CreateTemp("", "cdc_config")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tempFile.Name())

		content := []byte(`{
			"endpoint": "$TEST_ENDPOINT",
			"log_only": true
		}`)
		if _, err := tempFile.Write(content); err != nil {
			t.Fatal(err)
		}
		tempFile.Close()

		data, err := ReadConfigFile(tempFile.Name())
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify environment variable expansion
		var jsonCfg JSONConfig
		if err := json.Unmarshal(data, &jsonCfg); err != nil {
			t.Fatalf("Failed to parse JSON: %v", err)
		}

		if jsonCfg.Endpoint != "https://test.example.com/cdc" {
			t.Errorf("Expected endpoint 'https://test.example.com/cdc', got %s", jsonCfg.Endpoint)
		}
	})
}

func TestParseRetryPolicy(t *testing.T) {
	testCases := []struct {
		input    string
		expected RetryPolicy
		hasError bool
	}{
		{"linear", LinearRetryPolicy, false},
		{"exponential", ExponentialRetryPolicy, false},
		{"Linear", LinearRetryPolicy, false},
		{"EXPONENTIAL", ExponentialRetryPolicy, false},
		{"invalid", LinearRetryPolicy, true},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := parseRetryPolicy(tc.input)

			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != tc.expected {
					t.Errorf("Expected %d, got %d", tc.expected, result)
				}
			}
		})
	}
}
