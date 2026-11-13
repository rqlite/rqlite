package http_test

import (
	"os"
	"slices"
	"testing"

	"github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

func TestParseHostEnv(t *testing.T) {
	tests := []struct {
		name                 string
		envValue             string
		hasErr               bool
		expectedProtocol     string
		expectedHost         string
		expectedPort         uint16
		expectedAlternatives []string
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
			name:             "no port given",
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
			name:     "large port number",
			envValue: "http://localhost:99999999",
			hasErr:   true,
		},
		{
			name:     "negative port",
			envValue: "http://localhost:-1",
			hasErr:   true,
		},
		{
			name:     "invalid port",
			envValue: "http://localhost:port",
			hasErr:   true,
		},
		{
			name:     "invalid host",
			envValue: "http://[invalid:host",
			hasErr:   true,
		},
		{
			name:                 "two comma-delimited hosts",
			envValue:             "http://node1:4001,http://node2:4001",
			hasErr:               false,
			expectedProtocol:     "http",
			expectedHost:         "node1",
			expectedPort:         4001,
			expectedAlternatives: []string{"node2:4001"},
		},
		{
			name:                 "three comma-delimited hosts",
			envValue:             "https://node1:4001,https://node2:4001,https://node3:4003",
			hasErr:               false,
			expectedProtocol:     "https",
			expectedHost:         "node1",
			expectedPort:         4001,
			expectedAlternatives: []string{"node2:4001", "node3:4003"},
		},
		{
			name:                 "comma-delimited hosts with whitespace and empty entries",
			envValue:             " http://node1:4001, http://node2:4002 ,,",
			hasErr:               false,
			expectedProtocol:     "http",
			expectedHost:         "node1",
			expectedPort:         4001,
			expectedAlternatives: []string{"node2:4002"},
		},
		{
			name:                 "alternative host without scheme",
			envValue:             "https://node1:4001,//node2:4002",
			hasErr:               false,
			expectedProtocol:     "https",
			expectedHost:         "node1",
			expectedPort:         4001,
			expectedAlternatives: []string{"node2:4002"},
		},
		{
			name:                 "alternative host without port",
			envValue:             "http://node1:4001,http://node2",
			hasErr:               false,
			expectedProtocol:     "http",
			expectedHost:         "node1",
			expectedPort:         4001,
			expectedAlternatives: []string{"node2"},
		},
		{
			name:     "mismatched protocols",
			envValue: "https://node1:4001,http://node2:4001",
			hasErr:   true,
		},
		{
			name:     "invalid alternative host",
			envValue: "http://node1:4001,http://node2:port",
			hasErr:   true,
		},
		{
			name:     "only commas",
			envValue: ",,",
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
			protocol, host, port, alternatives, err := http.ParseHostEnv("RQLITE_HOST")

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
				if !slices.Equal(alternatives, tt.expectedAlternatives) {
					t.Errorf("expected alternatives %v, got %v", tt.expectedAlternatives, alternatives)
				}
			}
		})
	}
}

func TestParseCredentialsEnv(t *testing.T) {
	tests := []struct {
		name          string
		creds         string
		user          string
		password      string
		expectedCreds string
		expectedOK    bool
	}{
		{
			name:       "no variables set",
			expectedOK: false,
		},
		{
			name:          "credentials set",
			creds:         "alice:secret",
			expectedCreds: "alice:secret",
			expectedOK:    true,
		},
		{
			name:          "user and password set",
			user:          "alice",
			password:      "secret",
			expectedCreds: "alice:secret",
			expectedOK:    true,
		},
		{
			name:          "user set without password",
			user:          "alice",
			expectedCreds: "alice:",
			expectedOK:    true,
		},
		{
			name:       "password set without user",
			password:   "secret",
			expectedOK: false,
		},
		{
			name:          "credentials take precedence over user and password",
			creds:         "alice:secret",
			user:          "bob",
			password:      "other",
			expectedCreds: "alice:secret",
			expectedOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range []string{"RQLITE_CREDENTIALS", "RQLITE_USER", "RQLITE_PASSWORD"} {
				os.Unsetenv(v)
				defer os.Unsetenv(v)
			}
			if tt.creds != "" {
				os.Setenv("RQLITE_CREDENTIALS", tt.creds)
			}
			if tt.user != "" {
				os.Setenv("RQLITE_USER", tt.user)
			}
			if tt.password != "" {
				os.Setenv("RQLITE_PASSWORD", tt.password)
			}

			creds, ok := http.ParseCredentialsEnv("RQLITE_CREDENTIALS", "RQLITE_USER", "RQLITE_PASSWORD")
			if ok != tt.expectedOK {
				t.Errorf("expected ok %v, got %v", tt.expectedOK, ok)
			}
			if ok && creds != tt.expectedCreds {
				t.Errorf("expected credentials %q, got %q", tt.expectedCreds, creds)
			}
		})
	}
}

func TestParseBoolEnv(t *testing.T) {
	tests := []struct {
		name          string
		envValue      string
		set           bool
		hasErr        bool
		expectedValue bool
		expectedOK    bool
	}{
		{
			name:       "variable not set",
			set:        false,
			expectedOK: false,
		},
		{
			name:          "true",
			envValue:      "true",
			set:           true,
			expectedValue: true,
			expectedOK:    true,
		},
		{
			name:          "1",
			envValue:      "1",
			set:           true,
			expectedValue: true,
			expectedOK:    true,
		},
		{
			name:          "false",
			envValue:      "false",
			set:           true,
			expectedValue: false,
			expectedOK:    true,
		},
		{
			name:     "invalid value",
			envValue: "yes",
			set:      true,
			hasErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				os.Setenv("RQLITE_INSECURE", tt.envValue)
			} else {
				os.Unsetenv("RQLITE_INSECURE")
			}
			defer os.Unsetenv("RQLITE_INSECURE")

			value, ok, err := http.ParseBoolEnv("RQLITE_INSECURE")
			if tt.hasErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if ok != tt.expectedOK {
				t.Errorf("expected ok %v, got %v", tt.expectedOK, ok)
			}
			if ok && value != tt.expectedValue {
				t.Errorf("expected value %v, got %v", tt.expectedValue, value)
			}
		})
	}
}
