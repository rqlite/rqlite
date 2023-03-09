package tcp

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/rtls"
	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_NewDialer(t *testing.T) {
	d := NewDialer(1, nil)
	if d == nil {
		t.Fatal("failed to create a dialer")
	}
}

func Test_DialerNoConnect(t *testing.T) {
	d := NewDialer(87, nil)
	_, err := d.Dial("127.0.0.1:0", 5*time.Second)
	if err == nil {
		t.Fatalf("no error connecting to bad address")
	}
}

func Test_DialerHeader(t *testing.T) {
	s := mustNewEchoServer()
	defer s.Close()
	go s.Start(t)

	d := NewDialer(64, nil)
	conn, err := d.Dial(s.Addr(), 10*time.Second)
	if err != nil {
		t.Fatalf("failed to dial echo server: %s", err.Error())
	}

	buf := make([]byte, 1)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from echo server: %s", err.Error())
	}
	if exp, got := buf[0], byte(64); exp != got {
		t.Fatalf("got wrong response from echo server, exp %d, got %d", exp, got)
	}
}

func Test_DialerHeaderTLS(t *testing.T) {
	s, cert, key := mustNewEchoServerTLS()
	defer s.Close()
	defer os.Remove(cert)
	defer os.Remove(key)
	go s.Start(t)

	tlsConfig, err := rtls.CreateClientConfig("", "", "", true, false)
	if err != nil {
		t.Fatalf("failed to create TLS config: %s", err.Error())
	}
	d := NewDialer(23, tlsConfig)
	conn, err := d.Dial(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to dial TLS echo server: %s", err.Error())
	}

	buf := make([]byte, 1)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from TLS echo server: %s", err.Error())
	}
	if exp, got := buf[0], byte(23); exp != got {
		t.Fatalf("got wrong response from TLS echo server, exp %d, got %d", exp, got)
	}
}

func Test_DialerHeaderTLSBadConnect(t *testing.T) {
	s, cert, key := mustNewEchoServerTLS()
	defer s.Close()
	defer os.Remove(cert)
	defer os.Remove(key)
	go s.Start(t)

	// Connect to a TLS server with an unencrypted client, to make sure
	// code can handle that misconfig.
	d := NewDialer(56, nil)
	conn, err := d.Dial(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to dial TLS echo server: %s", err.Error())
	}

	buf := make([]byte, 1)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(buf)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("unexpected error from TLS echo server: %s", err.Error())
	}
}

type echoServer struct {
	ln net.Listener
}

// Addr returns the address of the echo server.
func (e *echoServer) Addr() string {
	return e.ln.Addr().String()
}

// Start starts the echo server.
func (e *echoServer) Start(t *testing.T) {
	for {
		conn, err := e.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// Successful testing can cause this.
				return
			}
			t.Logf("failed to accept a connection: %s", err.Error())
		}
		go func(c net.Conn) {
			buf := make([]byte, 1)
			_, err := c.Read(buf)
			if err != nil {
				return
			}
			_, err = c.Write(buf)
			if err != nil {
				t.Logf("failed to write byte: %s", err.Error())
			}
			c.Close()
		}(conn)
	}
}

// Close closes the echo server.
func (e *echoServer) Close() {
	e.ln.Close()
}

func mustNewEchoServer() *echoServer {
	return &echoServer{
		ln: mustTCPListener("127.0.0.1:0"),
	}
}

func mustNewEchoServerTLS() (*echoServer, string, string) {
	ln := mustTCPListener("127.0.0.1:0")
	cert := x509.CertFile("")
	key := x509.KeyFile("")

	tlsConfig, err := rtls.CreateServerConfig(cert, key, "", true, false)
	if err != nil {
		panic("failed to create TLS config")
	}

	return &echoServer{
		ln: tls.NewListener(ln, tlsConfig),
	}, cert, key
}
