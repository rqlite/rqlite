package main

import (
	"strings"
	"testing"
)

func TestConfigValidateHTTPAddresses(t *testing.T) {
	tests := []struct {
		name     string
		httpAddr string
		httpAdv  string
		wantErr  string
		wantAdv  string
	}{
		{
			name:     "multiple addresses with advertised address",
			httpAddr: "127.0.0.1:4001,127.0.0.2:4001",
			httpAdv:  "127.0.0.1:4001",
		},
		{
			name:     "multiple addresses require advertised address",
			httpAddr: "127.0.0.1:4001,127.0.0.2:4001",
			wantErr:  "multiple HTTP bind addresses require -http-adv-addr",
		},
		{
			name:     "one address still defaults advertised address",
			httpAddr: "127.0.0.1:4001",
			wantAdv:  "127.0.0.1:4001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				DataPath: t.TempDir(),
				HTTPAddr: tt.httpAddr,
				HTTPAdv:  tt.httpAdv,
				RaftAddr: "127.0.0.1:4002",
			}
			err := cfg.Validate()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("failed to validate config: %s", err)
			}
			if tt.wantAdv != "" && cfg.HTTPAdv != tt.wantAdv {
				t.Fatalf("expected advertised address %q, got %q", tt.wantAdv, cfg.HTTPAdv)
			}
		})
	}
}
