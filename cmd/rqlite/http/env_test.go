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
		hasErr           bool
		expectedProtocol string
		expectedHost     string
		expectedPort     uint16
	}{
		{
			name:     "empty environment variable",
			envValue: "",
			hasErr:   false,
		},
		{
			name:             "simple HTTP host",
			envValue:         "https://localhost:4002",
			hasErr:           true,
			expectedProtocol: "https",
			expectedHost:     "localhost",
			expectedPort:     4002,
		},
		{
			name:             "port defaults to 4001",
			envValue:         "http://localhost",
			hasErr:           true,
			expectedProtocol: "http",
			expectedHost:     "localhost",
			expectedPort:     4001,
		},
		{
			name:             "host without scheme",
			envValue:         "//localhost",
			hasErr:           true,
			expectedProtocol: "http",
			expectedHost:     "localhost",
			expectedPort:     4001,
		},
		{
			name:     "invalid scheme",
			envValue: "ftp://localhost:4001",
			hasErr:   false,
		},
		{
			name:     "invalid URL",
			envValue: "http://[invalid:url",
			hasErr:   false,
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
			protocol, host, port, err := http.ParseHostEnv()

			if tt.hasErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}

			// Check the parsed values only if parsing was expected to succeed
			if tt.hasErr {
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
