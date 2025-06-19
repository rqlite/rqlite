package http_test

import (
	"os"
	"testing"

	"github.com/rqlite/rqlite/v8/cmd/rqlite/http"
)

func TestParseHostEnv(t *testing.T) {
	tests := []struct {
		name             string
		envValue         string
		expectResult     bool
		expectedProtocol string
		expectedHost     string
		expectedPort     uint16
	}{
		{
			name:         "empty environment variable",
			envValue:     "",
			expectResult: false,
		},
		{
			name:             "simple HTTP host",
			envValue:         "https://localhost:4002",
			expectResult:     true,
			expectedProtocol: "https",
			expectedHost:     "localhost",
			expectedPort:     4002,
		},
		{
			name:             "port defaults to 4001",
			envValue:         "http://localhost",
			expectResult:     true,
			expectedProtocol: "http",
			expectedHost:     "localhost",
			expectedPort:     4001,
		},
		{
			name:             "host without scheme",
			envValue:         "//localhost",
			expectResult:     true,
			expectedProtocol: "http",
			expectedHost:     "localhost",
			expectedPort:     4001,
		},
		{
			name:         "invalid scheme",
			envValue:     "ftp://localhost:4001",
			expectResult: false,
		},
		{
			name:         "invalid URL",
			envValue:     "http://[invalid:url",
			expectResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up environment
			if tt.envValue != "" {
				os.Setenv("RQLITE_HOST", tt.envValue)
			} else {
				os.Unsetenv("RQLITE_HOST")
			}
			defer os.Unsetenv("RQLITE_HOST")

			// Call the function
			protocol, host, port, success := http.ParseHostEnv()

			// Check the result
			if success != tt.expectResult {
				t.Errorf("expected result %v, got %v", tt.expectResult, success)
			}

			// Check the parsed values only if parsing was expected to succeed
			if tt.expectResult {
				if protocol != tt.expectedProtocol {
					t.Errorf("expected protocol %q, got %q", tt.expectedProtocol, protocol)
				}
				if host != tt.expectedHost {
					t.Errorf("expected host %q, got %q", tt.expectedHost, host)
				}
				if port != tt.expectedPort {
					t.Errorf("expected port %d, got %d", tt.expectedPort, port)
				}
			}
		})
	}
}
