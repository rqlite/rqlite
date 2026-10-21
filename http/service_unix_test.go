//go:build unix

package http

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auth"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/rtls"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/store"
)

func Test_UnixSocketService(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.UnixSocket = unixSocketPath(t)
	s.BuildInfo = map[string]any{"version": "unix-test"}
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)

	if fi, err := os.Stat(s.UnixSocket); err != nil || fi.Mode()&os.ModeSocket == 0 {
		t.Fatalf("expected socket file, got %v, %v", fi, err)
	}
	if s.Addr().Network() != "tcp" {
		t.Fatalf("socket changed TCP service address: %s", s.Addr())
	}

	for _, tt := range []struct {
		name   string
		client *http.Client
		url    string
	}{
		{"TCP", &http.Client{Timeout: 5 * time.Second}, "http://" + s.Addr().String()},
		{"Unix", unixSocketClient(t, s.UnixSocket), "http://localhost"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, path := range []string{"/status", "/db/query?level=none&q=SELECT%201"} {
				resp, err := tt.client.Get(tt.url + path)
				if err != nil {
					t.Fatalf("failed to get %s: %s", path, err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || resp.Header.Get(VersionHTTPHeader) != "unix-test" {
					t.Fatalf("unexpected response to %s: %s, %v", path, resp.Status, resp.Header)
				}
			}
			resp, err := tt.client.Post(tt.url+"/db/execute", "application/json", strings.NewReader(`["INSERT INTO foo VALUES(1)"]`))
			if err != nil {
				t.Fatalf("failed to execute: %s", err)
			}
			defer resp.Body.Close()
			if body := mustReadBody(t, resp); resp.StatusCode != http.StatusOK || body != `{"results":[]}` {
				t.Fatalf("unexpected execute response: %s, %s", resp.Status, body)
			}
		})
	}
}

func Test_UnixSocketAuth(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	creds := &mockCredentialStore{aaFunc: func(username, password, perm string) bool {
		return username == "reader" && password == "secret" && perm == auth.PermQuery
	}}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), creds)
	s.UnixSocket = unixSocketPath(t)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)
	client := unixSocketClient(t, s.UnixSocket)

	for _, tt := range []struct {
		name     string
		path     string
		password string
		status   int
	}{
		{"no credentials", "/db/query?level=none&q=SELECT%201", "", http.StatusUnauthorized},
		{"bad password", "/db/query?level=none&q=SELECT%201", "wrong", http.StatusUnauthorized},
		{"authorized query", "/db/query?level=none&q=SELECT%201", "secret", http.StatusOK},
		{"unauthorized status", "/status", "secret", http.StatusUnauthorized},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := mustNewHTTPRequest("http://localhost" + tt.path)
			if tt.password != "" {
				req.SetBasicAuth("reader", tt.password)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("failed to make request: %s", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.status {
				t.Fatalf("expected status %d, got %s", tt.status, resp.Status)
			}
		})
	}
}

func Test_UnixSocketHTTPS(t *testing.T) {
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.UnixSocket = unixSocketPath(t)
	cert, key, err := rtls.GenerateSelfSignedCert(pkix.Name{CommonName: "rqlite"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate certificate: %s", err)
	}
	s.CertFile = mustWriteTempFile(t, cert)
	s.KeyFile = mustWriteTempFile(t, key)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)

	// Enabling HTTPS on TCP does not change the socket's plaintext HTTP API.
	resp, err := unixSocketClient(t, s.UnixSocket).Get("http://localhost/status")
	if err != nil {
		t.Fatalf("failed to make socket request: %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.TLS != nil {
		t.Fatalf("unexpected plaintext response: %s, TLS %v", resp.Status, resp.TLS)
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err = client.Get("https://" + s.Addr().String() + "/status")
	if err != nil {
		t.Fatalf("failed to make HTTPS request: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.TLS == nil {
		t.Fatalf("unexpected HTTPS response: %s, TLS %v", resp.Status, resp.TLS)
	}
}

func Test_UnixSocketMutualTLS(t *testing.T) {
	caCertPEM, caKeyPEM, err := rtls.GenerateCACert(pkix.Name{CommonName: "ca.rqlite.io"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA certificate: %s", err)
	}
	ca, err := tls.X509KeyPair(caCertPEM, caKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := x509.ParseCertificate(ca.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	newCert := func(t *testing.T, cn string) ([]byte, []byte) {
		t.Helper()
		cert, key, err := rtls.GenerateCertIPSAN(pkix.Name{CommonName: cn}, time.Hour, 2048, parent, ca.PrivateKey, net.ParseIP("127.0.0.1"))
		if err != nil {
			t.Fatalf("failed to generate certificate: %s", err)
		}
		return cert, key
	}
	cert, key := newCert(t, "server.rqlite.io")
	m := &MockStore{}
	c := &mockClusterService{}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.UnixSocket = unixSocketPath(t)
	s.CertFile = mustWriteTempFile(t, cert)
	s.KeyFile = mustWriteTempFile(t, key)
	s.CACertFile = mustWriteTempFile(t, caCertPEM)
	s.ClientVerify = true
	s.ClientVerifyCommonName = "client.rqlite.io"
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)

	// The TCP listener continues to accept mTLS clients while the socket is enabled.
	cfg := &tls.Config{RootCAs: x509.NewCertPool()}
	if !cfg.RootCAs.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("failed to add trusted CA")
	}
	cert, key = newCert(t, s.ClientVerifyCommonName)
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Certificates = []tls.Certificate{pair}
	tr := &http.Transport{TLSClientConfig: cfg}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err := client.Get("https://" + s.Addr().String() + "/status")
	if err != nil {
		t.Fatalf("mTLS request failed with Unix socket enabled: %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.TLS == nil {
		t.Fatalf("unexpected HTTPS response: %s, TLS %v", resp.Status, resp.TLS)
	}

	// The same service also accepts plaintext HTTP over the Unix socket.
	resp, err = unixSocketClient(t, s.UnixSocket).Get("http://localhost/status")
	if err != nil {
		t.Fatalf("failed to make socket request without a certificate: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.TLS != nil {
		t.Fatalf("unexpected plaintext response: %s, TLS %v", resp.Status, resp.TLS)
	}
}

func Test_UnixSocketForwarding(t *testing.T) {
	m := &MockStore{leaderAddr: "leader:4002"}
	m.queryFn = func(*command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return nil, 0, store.ErrNotLeader
	}
	c := &mockClusterService{apiAddr: "http://leader:4001"}
	c.queryFn = func(qr *command.QueryRequest, addr string, timeout time.Duration) ([]*command.QueryRows, uint64, error) {
		if addr != m.leaderAddr {
			t.Errorf("forwarded to %s instead of %s", addr, m.leaderAddr)
		}
		return nil, 0, nil
	}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.UnixSocket = unixSocketPath(t)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service: %s", err)
	}
	t.Cleanup(s.Close)
	client := unixSocketClient(t, s.UnixSocket)
	resp, err := client.Get("http://localhost/db/query?level=weak&q=SELECT%201")
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get(ServedByHTTPHeader) != m.leaderAddr {
		t.Fatalf("unexpected forwarded response: %s, %v", resp.Status, resp.Header)
	}

	// Explicit redirects still use the leader's advertised network URL.
	resp, err = client.Get("http://localhost/db/query?redirect&level=weak&q=SELECT%201")
	if err != nil {
		t.Fatalf("failed to query with redirect: %s", err)
	}
	defer resp.Body.Close()
	const location = "http://leader:4001/db/query?redirect&level=weak&q=SELECT%201"
	if resp.StatusCode != http.StatusMovedPermanently || resp.Header.Get("Location") != location {
		t.Fatalf("unexpected redirect: %s, %v", resp.Status, resp.Header)
	}
}

func Test_UnixSocketExistingPath(t *testing.T) {
	for _, kind := range []string{"file", "active socket", "stale socket"} {
		t.Run(kind, func(t *testing.T) {
			path := unixSocketPath(t)
			if kind == "file" {
				if err := os.WriteFile(path, []byte("do not replace"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { ln.Close() })
				if kind == "stale socket" {
					ln.SetUnlinkOnClose(false)
					ln.Close()
				}
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			m := &MockStore{}
			c := &mockClusterService{}
			s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
			s.UnixSocket = path
			if err := s.Start(); err == nil || !strings.Contains(err.Error(), path) {
				if err == nil {
					s.Close()
				}
				t.Fatalf("expected startup error naming socket path, got %v", err)
			}
			after, err := os.Stat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("existing path was modified: %v", err)
			}
			if kind == "file" {
				b, err := os.ReadFile(path)
				if err != nil || string(b) != "do not replace" {
					t.Fatalf("file contents changed: %q, %v", b, err)
				}
			}
			s.ln.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
			if _, err := s.ln.Accept(); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("TCP listener not closed after startup failure: %v", err)
			}
		})
	}
}

func Test_UnixSocketStartErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		path string
	}{
		{"missing parent", filepath.Join(unixSocketPath(t), "http.sock")},
		{"relative path", "http.sock"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := &MockStore{}
			c := &mockClusterService{}
			s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
			s.UnixSocket = tt.path
			if err := s.Start(); err == nil {
				s.Close()
				t.Fatal("expected startup error")
			}
		})
	}
}

func Test_UnixSocketClose(t *testing.T) {
	path := unixSocketPath(t)
	// Also exercise Close immediately after Start, before Serve necessarily runs.
	for i := 0; i < 10; i++ {
		m := &MockStore{}
		c := &mockClusterService{}
		s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
		s.UnixSocket = path
		if err := s.Start(); err != nil {
			t.Fatalf("failed to start service on iteration %d: %s", i, err)
		}
		s.Close()
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("socket not removed on close: %v", err)
		}
		s.ln.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
		if _, err := s.ln.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("TCP listener not closed: %v", err)
		}
		s.unixLn.(*net.UnixListener).SetDeadline(time.Now().Add(time.Second))
		if _, err := s.unixLn.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Unix listener not closed: %v", err)
		}
	}
}

func unixSocketPath(t *testing.T) string {
	t.Helper()
	// Keep the path short enough for Unix sockets, including on macOS.
	dir, err := os.MkdirTemp("", "http-unix-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fsutil.RemoveAll(dir) })
	return filepath.Join(dir, "http.sock")
}

func unixSocketClient(t *testing.T, path string) *http.Client {
	t.Helper()
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
