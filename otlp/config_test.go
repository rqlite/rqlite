package otlp

import (
	"crypto/x509/pkix"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rtls"
)

func Test_ConfigValidate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid minimal",
			cfg:  Config{Endpoint: "localhost:4317", Interval: DefaultInterval},
		},
		{
			name: "valid insecure",
			cfg:  Config{Endpoint: "localhost:4317", Interval: DefaultInterval, Insecure: true},
		},
		{
			name: "valid TLS",
			cfg: Config{Endpoint: "localhost:4317", Interval: DefaultInterval,
				CACertFile: "ca.pem", CertFile: "cert.pem", KeyFile: "key.pem"},
		},
		{
			name:    "empty endpoint",
			cfg:     Config{Interval: DefaultInterval},
			wantErr: true,
		},
		{
			name:    "endpoint with scheme",
			cfg:     Config{Endpoint: "http://localhost:4317", Interval: DefaultInterval},
			wantErr: true,
		},
		{
			name:    "endpoint missing port",
			cfg:     Config{Endpoint: "localhost", Interval: DefaultInterval},
			wantErr: true,
		},
		{
			name:    "zero interval",
			cfg:     Config{Endpoint: "localhost:4317"},
			wantErr: true,
		},
		{
			name:    "negative interval",
			cfg:     Config{Endpoint: "localhost:4317", Interval: -time.Second},
			wantErr: true,
		},
		{
			name:    "cert without key",
			cfg:     Config{Endpoint: "localhost:4317", Interval: DefaultInterval, CertFile: "cert.pem"},
			wantErr: true,
		},
		{
			name:    "key without cert",
			cfg:     Config{Endpoint: "localhost:4317", Interval: DefaultInterval, KeyFile: "key.pem"},
			wantErr: true,
		},
		{
			name: "insecure with skip-verify",
			cfg: Config{Endpoint: "localhost:4317", Interval: DefaultInterval,
				Insecure: true, InsecureSkipVerify: true},
			wantErr: true,
		},
		{
			name: "insecure with CA cert",
			cfg: Config{Endpoint: "localhost:4317", Interval: DefaultInterval,
				Insecure: true, CACertFile: "ca.pem"},
			wantErr: true,
		},
		{
			name: "insecure with cert and key",
			cfg: Config{Endpoint: "localhost:4317", Interval: DefaultInterval,
				Insecure: true, CertFile: "cert.pem", KeyFile: "key.pem"},
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func Test_ConfigTLSConfig(t *testing.T) {
	c := &Config{Endpoint: "localhost:4317", Interval: DefaultInterval, Insecure: true}
	tlsCfg, err := c.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if tlsCfg != nil {
		t.Fatalf("expected nil TLS config for insecure connection")
	}

	c = &Config{Endpoint: "localhost:4317", Interval: DefaultInterval}
	tlsCfg, err = c.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if tlsCfg == nil {
		t.Fatalf("expected non-nil TLS config")
	}
	if tlsCfg.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be false")
	}

	c.InsecureSkipVerify = true
	tlsCfg, err = c.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if !tlsCfg.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be true")
	}
}

func Test_ConfigTLSConfig_MutualTLS(t *testing.T) {
	certPEM, keyPEM, err := rtls.GenerateSelfSignedCert(pkix.Name{CommonName: "rqlite.test"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate cert: %s", err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert: %s", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key: %s", err)
	}

	c := &Config{Endpoint: "localhost:4317", Interval: DefaultInterval,
		CACertFile: certFile, CertFile: certFile, KeyFile: keyFile}
	tlsCfg, err := c.TLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 client certificate, got %d", len(tlsCfg.Certificates))
	}
	if tlsCfg.RootCAs == nil {
		t.Fatalf("expected non-nil RootCAs")
	}

	c = &Config{Endpoint: "localhost:4317", Interval: DefaultInterval,
		CertFile: filepath.Join(dir, "na.pem"), KeyFile: filepath.Join(dir, "na-key.pem")}
	if _, err := c.TLSConfig(); err == nil {
		t.Fatalf("expected error for nonexistent cert files")
	}
}
