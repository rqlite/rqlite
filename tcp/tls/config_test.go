package tls

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"os"
	"testing"

	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_ClientConnectServer(t *testing.T) {
	serverCertFile := x509.ServerCASignedCertFile("")
	serverKeyFile := x509.ServerPrivateKeyFile("")
	defer os.Remove(serverCertFile)
	defer os.Remove(serverKeyFile)

	serverConfig, err := CreateServerConfig(serverCertFile, serverKeyFile, "", false)
	if err != nil {
		t.Fatalf("failed to create server TLS config: %s", err.Error())
	}

	s := mustNewEchoServerTLS(serverConfig)
	defer s.Close()
	go s.Start(t)

	clientCAFile := x509.CACertFile("")
	defer os.Remove(clientCAFile)

	clientConfig, err := CreateClientConfig(clientCAFile, false)
	if err != nil {
		t.Fatalf("failed to create client TLS config: %s", err.Error())
	}
	dialer := &net.Dialer{
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				panic("Dialing")
				return net.Dial(network, address)
			},
		},
	}

	// NOT WORKING -- might need to test with HTTP, which has more control over dialer..
	// https://stackoverflow.com/questions/40624248/golang-force-http-request-to-specific-ip-similar-to-curl-resolve

	conn, err := tls.DialWithDialer(dialer, "tcp", s.Addr(), clientConfig)
	if err != nil {
		t.Fatalf("failed to dial echo server: %s", err.Error())
	}
	defer conn.Close()
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

func mustNewEchoServerTLS(cfg *tls.Config) *echoServer {
	ln := mustTCPListener("127.0.0.1:0")
	l := tls.NewListener(ln, cfg)
	return &echoServer{
		ln: l,
	}
}

// mustTCPListener returns a listener on bind, or panics.
func mustTCPListener(bind string) net.Listener {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		panic(err)
	}
	return l
}
