package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func Test_HTTPUnixSocketConfig(t *testing.T) {
	for _, tt := range []struct {
		name         string
		args         []string
		err          string
		verifyClient bool
		commonName   string
	}{
		{name: "disabled"},
		{name: "enabled", args: []string{"-http-unix-socket", filepath.Join(t.TempDir(), "rqlite.sock")}},
		{name: "relative path", args: []string{"-http-unix-socket", "rqlite.sock"}, err: "absolute filesystem path"},
		{name: "URL", args: []string{"-http-unix-socket", "unix:///run/rqlite.sock"}, err: "absolute filesystem path"},
		{name: "mutual TLS", args: []string{"-http-unix-socket", filepath.Join(t.TempDir(), "rqlite.sock"), "-http-verify-client"}, verifyClient: true},
		{name: "mutual TLS with common name", args: []string{"-http-unix-socket", filepath.Join(t.TempDir(), "rqlite.sock"), "-http-verify-client", "-http-verify-common-name", "client.rqlite.io"}, verifyClient: true, commonName: "client.rqlite.io"},
		{name: "common name without mutual TLS", args: []string{"-http-unix-socket", filepath.Join(t.TempDir(), "rqlite.sock"), "-http-verify-common-name", "client.rqlite.io"}, err: "-http-verify-common-name requires -http-verify-client"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append(append([]string(nil), tt.args...), t.TempDir())
			_, cfg, err := Forge(args)
			if err != nil {
				t.Fatalf("failed to parse config: %s", err)
			}
			err = cfg.Validate()
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("expected error containing %q, got %v", tt.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("failed to validate config: %s", err)
			}
			if tt.name == "disabled" && cfg.HTTPUnixSocket != "" {
				t.Fatalf("Unix socket enabled by default: %s", cfg.HTTPUnixSocket)
			}
			if len(tt.args) > 0 && cfg.HTTPUnixSocket != tt.args[1] {
				t.Fatalf("incorrect socket path: %s", cfg.HTTPUnixSocket)
			}
			if cfg.HTTPVerifyClient != tt.verifyClient || cfg.HTTPVerifyCommonName != tt.commonName {
				t.Fatalf("Unix socket changed client verification settings: %v, %q", cfg.HTTPVerifyClient, cfg.HTTPVerifyCommonName)
			}
			if cfg.HTTPAdv != cfg.HTTPAddr || cfg.HTTPURL() != "http://localhost:4001" {
				t.Fatalf("Unix socket changed advertised HTTP address: %s", cfg.HTTPURL())
			}
		})
	}
}
