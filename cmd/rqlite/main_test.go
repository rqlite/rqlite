package main

import (
	"os"
	"testing"
)

func TestParseHostEnv(t *testing.T) {
	tests := []struct {
		name           string
		envValue       string
		initialHost    string
		initialPort    uint16
		initialProto   string
		expectedHost   string
		expectedPort   uint16
		expectedProto  string
		result         bool
	}{
		{
			name:           "empty environment variable",
			envValue:       "",
			initialHost:    "127.0.0.1",
			initialPort:    4001,
			initialProto:   "http",
			expectedHost:   "127.0.0.1",
			expectedPort:   4001,
			expectedProto:  "http",
			result:         false,
		},
		{
			name:           "host only",
			envValue:       "example.com",
			initialHost:    "127.0.0.1",
			initialPort:    4001,
			initialProto:   "http",
			expectedHost:   "example.com",
			expectedPort:   4001,
			expectedProto:  "http",
			result:         true,
		},
		{
			name:           "host and port",
			envValue:       "example.com:8080",
			initialHost:    "127.0.0.1",
			initialPort:    4001,
			initialProto:   "http",
			expectedHost:   "example.com",
			expectedPort:   8080,
			expectedProto:  "http",
			result:         true,
		},
		{
			name:           "scheme, host, and port",
			envValue:       "https://example.com:8443",
			initialHost:    "127.0.0.1",
			initialPort:    4001,
			initialProto:   "http",
			expectedHost:   "example.com",
			expectedPort:   8443,
			expectedProto:  "https",
			result:         true,
		},
		{
			name:           "command line flag overrides env - host",
			envValue:       "example.com",
			initialHost:    "custom.host", // Custom value from command line
			initialPort:    4001,
			initialProto:   "http",
			expectedHost:   "custom.host", // Should not be changed
			expectedPort:   4001,
			expectedProto:  "http",
			result:         true,
		},
		{
			name:           "command line flag overrides env - port",
			envValue:       "example.com:8080",
			initialHost:    "127.0.0.1",
			initialPort:    5000, // Custom value from command line
			initialProto:   "http",
			expectedHost:   "example.com",
			expectedPort:   5000, // Should not be changed
			expectedProto:  "http",
			result:         true,
		},
		{
			name:           "command line flag overrides env - scheme",
			envValue:       "https://example.com",
			initialHost:    "127.0.0.1",
			initialPort:    4001,
			initialProto:   "https", // Custom value from command line
			expectedHost:   "example.com",
			expectedPort:   4001,
			expectedProto:  "https", // Should not be changed
			result:         true,
		},
		{
			name:           "invalid port number",
			envValue:       "example.com:invalid",
			initialHost:    "127.0.0.1",
			initialPort:    4001,
			initialProto:   "http",
			expectedHost:   "example.com",
			expectedPort:   4001, // Default value not changed due to invalid port
			expectedProto:  "http",
			result:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up the environment
			if tt.envValue != "" {
				os.Setenv("RQLITE_HOST", tt.envValue)
				defer os.Unsetenv("RQLITE_HOST")
			} else {
				os.Unsetenv("RQLITE_HOST")
			}

			// Create a test argT with initial values
			argv := &argT{
				Protocol: tt.initialProto,
				Host:     tt.initialHost,
				Port:     tt.initialPort,
			}

			// Call the function
			result := parseHostEnv(argv)

			// Check the results
			if result != tt.result {
				t.Errorf("parseHostEnv() = %v, want %v", result, tt.result)
			}
			if argv.Host != tt.expectedHost {
				t.Errorf("Host = %v, want %v", argv.Host, tt.expectedHost)
			}
			if argv.Port != tt.expectedPort {
				t.Errorf("Port = %v, want %v", argv.Port, tt.expectedPort)
			}
			if argv.Protocol != tt.expectedProto {
				t.Errorf("Protocol = %v, want %v", argv.Protocol, tt.expectedProto)
			}
		})
	}
}