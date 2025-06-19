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
			hasErr:   true,
		},
		{
			name:             "simple HTTP host",
			envValue:         "https://localhost:4002",
			hasErr:           false,
			expectedProtocol: "https",
			expectedHost:     "localhost",
			expectedPort:     4002,
		},
		{
			name:             "port defaults to 4001",
			envValue:         "http://localhost",
			hasErr:           false,
			expectedProtocol: "http",
			expectedHost:     "localhost",
			expectedPort:     0,
		},
		{
			name:             "host without scheme",
			envValue:         "//localhost",
			hasErr:           false,
			expectedProtocol: "",
			expectedHost:     "localhost",
			expectedPort:     0,
		},
		{
			name:     "invalid scheme",
			envValue: "ftp://localhost:4001",
			hasErr:   true,
		},
		{
			name:     "invalid URL",
			envValue: "http://[invalid:url",
			hasErr:   true,
		},
		{
			name:     "invalid port",
			envValue: "http://localhost:99999999",
			hasErr:   true,
		},
		{
			name:     "invalid host",
			envValue: "http://[invalid:host",
			hasErr:   true,
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
			protocol, host, port, err := http.ParseHostEnv("RQLITE_HOST")

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
			if !tt.hasErr {
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
