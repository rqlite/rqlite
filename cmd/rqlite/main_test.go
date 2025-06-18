package main

import (
	"os"
	"testing"
)

func TestParseHostEnv(t *testing.T) {
	tests := []struct {
		name         string
		envValue     string
		expectResult bool
		expectArgs   argT
	}{
		{
			name:         "empty environment variable",
			envValue:     "",
			expectResult: false,
			expectArgs:   argT{},
		},
		{
			name:         "simple HTTP host",
			envValue:     "https://localhost:4002",
			expectResult: true,
			expectArgs: argT{
				Protocol: "https",
				Host:     "localhost",
				Port:     4002,
			},
		},
		{
			name:         "port defaults to 4001",
			envValue:     "http://localhost",
			expectResult: true,
			expectArgs: argT{
				Protocol: "http",
				Host:     "localhost",
				Port:     4001,
			},
		},
		{
			name:         "host without scheme",
			envValue:     "//localhost",
			expectResult: true,
			expectArgs: argT{
				Protocol: "http",
				Host:     "localhost",
				Port:     4001,
			},
		},
		{
			name:         "invalid scheme",
			envValue:     "ftp://localhost:4001",
			expectResult: false,
			expectArgs:   argT{},
		},
		{
			name:         "invalid URL",
			envValue:     "http://[invalid:url",
			expectResult: false,
			expectArgs:   argT{},
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

			// Create a fresh argT instance
			argv := &argT{}

			// Call the function
			result := parseHostEnv(argv)

			// Check the result
			if result != tt.expectResult {
				t.Errorf("expected result %v, got %v", tt.expectResult, result)
			}

			// Check the parsed values only if parsing was expected to succeed
			if tt.expectResult {
				if argv.Protocol != tt.expectArgs.Protocol {
					t.Errorf("expected protocol %q, got %q", tt.expectArgs.Protocol, argv.Protocol)
				}
				if argv.Host != tt.expectArgs.Host {
					t.Errorf("expected host %q, got %q", tt.expectArgs.Host, argv.Host)
				}
				if argv.Port != tt.expectArgs.Port {
					t.Errorf("expected port %d, got %d", tt.expectArgs.Port, argv.Port)
				}
			}
		})
	}
}
