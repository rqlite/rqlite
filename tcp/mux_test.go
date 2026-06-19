package tcp

import (
	"bytes"
	"crypto/tls"
	cryptox509 "crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rtls"
	"github.com/rqlite/rqlite/v10/testdata/x509"
)

// Ensure the muxer can split a listener's connections across multiple listeners.
func TestMux(t *testing.T) {
	if err := quick.Check(func(n uint8, msg []byte) bool {
		if testing.Verbose() {
			if len(msg) == 0 {
				log.Printf("n=%d, <no message>", n)
			} else {
				log.Printf("n=%d, hdr=%d, len=%d", n, msg[0], len(msg))
			}
		}

		var wg sync.WaitGroup

		// Open single listener on random port.
		tcpListener := mustTCPListener("127.0.0.1:0")
		defer tcpListener.Close()

		// Setup muxer & listeners.
		mux, err := NewMux(tcpListener, nil)
		if err != nil {
			t.Fatalf("failed to create mux: %s", err.Error())
		}
		defer mux.Close()
		mux.Timeout = 200 * time.Millisecond
		if !testing.Verbose() {
			mux.Logger = log.New(io.Discard, "", 0)
		}
		for i := range n {
			ln := mux.Listen(i)

			wg.Add(1)
			go func(i uint8, ln net.Listener) {
				defer wg.Done()

				// Wait for a connection for this listener.
				conn, err := ln.Accept()
				if conn != nil {
					defer conn.Close()
				}

				// If there is no message or the header byte
				// doesn't match then expect close.
				if len(msg) == 0 || msg[0] != i {
					if err == nil || err.Error() != "network connection closed" {
						t.Logf("unexpected error: %s", err)
					}
					return
				}

				// If the header byte matches this listener
				// then expect a connection and read the message.
				var buf bytes.Buffer
				if _, err := io.CopyN(&buf, conn, int64(len(msg)-1)); err != nil {
					t.Log(err)
				} else if !bytes.Equal(msg[1:], buf.Bytes()) {
					t.Logf("message mismatch:\n\nexp=%x\n\ngot=%x\n\n", msg[1:], buf.Bytes())
				}

				// Write response.
				if _, err := conn.Write([]byte("OK")); err != nil {
					t.Log(err)
				}
			}(i, ln)
		}

		// Begin serving from the listener.
		go mux.Serve()

		// Write message to TCP listener and read OK response.
		conn, err := net.Dial("tcp", tcpListener.Addr().String())
		if err != nil {
			t.Fatal(err)
		} else if _, err = conn.Write(msg); err != nil {
			t.Fatal(err)
		}

		// Read the response into the buffer.
		var resp [2]byte
		_, err = io.ReadFull(conn, resp[:])

		// If the message header is less than n then expect a response.
		// Otherwise, we should get an EOF because the mux closed.
		if len(msg) > 0 && msg[0] < n {
			if string(resp[:]) != `OK` {
				t.Fatalf("unexpected response: %s", resp[:])
			}
		} else {
			if err == nil || (err != io.EOF && !(strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "closed by the remote host"))) {
				t.Fatalf("unexpected error: %s", err)
			}
		}

		// Close connection.
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}

		// Close original TCP listener and wait for all goroutines to close.
		tcpListener.Close()
		wg.Wait()

		return true
	}, nil); err != nil {
		t.Error(err)
	}
}

func TestMux_Advertise(t *testing.T) {
	// Setup muxer.
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	addr := &mockAddr{
		Nwk:  "tcp",
		Addr: "rqlite.com:8081",
	}

	mux, err := NewMux(tcpListener, addr)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()
	mux.Timeout = 200 * time.Millisecond
	if !testing.Verbose() {
		mux.Logger = log.New(io.Discard, "", 0)
	}

	ln := mux.Listen(1)
	if ln.Addr().String() != addr.Addr {
		t.Fatalf("listener advertise address not correct, exp %s, got %s",
			ln.Addr().String(), addr.Addr)
	}
}

// Ensure two handlers cannot be registered for the same header byte.
func TestMux_Listen_ErrAlreadyRegistered(t *testing.T) {
	defer func() {
		if r := recover(); r != `listener already registered under header byte: 5` {
			t.Fatalf("unexpected recover: %#v", r)
		}
	}()

	// Register two listeners with the same header byte.
	tcpListener := mustTCPListener("127.0.0.1:0")
	mux, err := NewMux(tcpListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()
	mux.Listen(5)
	mux.Listen(5)
}

func TestTLSMux(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	cert := x509.CertExampleDotComFile("")
	defer os.Remove(cert)
	key := x509.KeyExampleDotComFile("")
	defer os.Remove(key)

	mux, err := NewTLSMux(tcpListener, nil, cert, key)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	defer mux.Close()
	go mux.Serve()

	// Verify that the listener is secured.
	conn, err := tls.Dial("tcp", tcpListener.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	state := conn.ConnectionState()
	if !state.HandshakeComplete {
		t.Fatal("connection handshake failed to complete")
	}
	if state.PeerCertificates[0].Subject.CommonName != "example.com" {
		t.Fatalf("unexpected common name: %s", state.PeerCertificates[0].Subject.CommonName)
	}

	// Next swap in a new cert and key, and verify that the new cert is used.
	cert2 := x509.CertExample2DotComFile("")
	defer os.Remove(cert2)
	key2 := x509.KeyExample2DotComFile("")
	defer os.Remove(key2)
	mustRename(key, key2)
	mustRename(cert, cert2)

	// Verify that the listener is still secured.
	conn, err = tls.Dial("tcp", tcpListener.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	state = conn.ConnectionState()
	if !state.HandshakeComplete {
		t.Fatal("connection handshake failed to complete")
	}
	if state.PeerCertificates[0].Subject.CommonName != "example2.com" {
		t.Fatalf("unexpected common name: %s", state.PeerCertificates[0].Subject.CommonName)
	}
}

func TestTLSMux_Fail(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()
	_, err := NewTLSMux(tcpListener, nil, "xxxx", "yyyy")
	if err == nil {
		t.Fatalf("created mux unexpectedly with bad resources")
	}
}

func TestMutualTLSMux(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	cert := x509.CertExampleDotComFile("")
	defer os.Remove(cert)
	key := x509.KeyExampleDotComFile("")
	defer os.Remove(key)
	caCert := x509.CertMyCAFile("")
	defer os.Remove(caCert)

	mux, err := NewMutualTLSMux(tcpListener, nil, cert, key, caCert, rtls.NoVerifyCN)
	if err != nil {
		t.Fatalf("failed to create mutual TLS mux: %s", err.Error())
	}
	defer mux.Close()
	go mux.Serve()

	if mux.tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected RequireAndVerifyClientCert, got %v", mux.tlsConfig.ClientAuth)
	}

	conn, err := tls.Dial("tcp", tcpListener.Addr().String(), &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Ensure mutual TLS is being enforced.
	var b [1]byte
	_, err = conn.Read(b[:])
	if err == nil {
		t.Fatalf("expected error reading from mux enforcing mutual TLS, got nil")
	}
	if !strings.Contains(err.Error(), "certificate required") {
		t.Fatalf("expected error to reference missing client certificate, got %s", err.Error())
	}
}

func TestMutualTLSMuxVerifyCommonName(t *testing.T) {
	// Generate a CA and three CA-signed certs (server, allowed client, denied client).
	caCertPEM, caKeyPEM, err := rtls.GenerateCACert(pkix.Name{CommonName: "ca.rqlite.io"}, time.Hour, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA cert: %s", err)
	}
	caCertBlock, _ := pem.Decode(caCertPEM)
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	parsedCACert, err := cryptox509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		t.Fatalf("failed to parse CA cert: %s", err)
	}
	parsedCAKey, err := cryptox509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("failed to parse CA key: %s", err)
	}

	serverIP := net.ParseIP("127.0.0.1")
	serverCertPEM, serverKeyPEM, err := rtls.GenerateCertIPSAN(pkix.Name{CommonName: "server.rqlite.io"}, time.Hour, 2048, parsedCACert, parsedCAKey, serverIP)
	if err != nil {
		t.Fatalf("failed to generate server cert: %s", err)
	}
	allowedPEM, allowedKeyPEM, err := rtls.GenerateCertIPSAN(pkix.Name{CommonName: "allowed.rqlite.io"}, time.Hour, 2048, parsedCACert, parsedCAKey, serverIP)
	if err != nil {
		t.Fatalf("failed to generate allowed client cert: %s", err)
	}
	deniedPEM, deniedKeyPEM, err := rtls.GenerateCertIPSAN(pkix.Name{CommonName: "denied.rqlite.io"}, time.Hour, 2048, parsedCACert, parsedCAKey, serverIP)
	if err != nil {
		t.Fatalf("failed to generate denied client cert: %s", err)
	}

	mustWrite := func(b []byte) string {
		f, err := os.CreateTemp(t.TempDir(), "rqlite-mux-common-name")
		if err != nil {
			t.Fatalf("failed to create temp file: %s", err)
		}
		if _, err := f.Write(b); err != nil {
			t.Fatalf("failed to write temp file: %s", err)
		}
		f.Close()
		return f.Name()
	}

	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	mux, err := NewMutualTLSMux(tcpListener, nil, mustWrite(serverCertPEM), mustWrite(serverKeyPEM), mustWrite(caCertPEM), "allowed.rqlite.io")
	if err != nil {
		t.Fatalf("failed to create mutual TLS mux: %s", err.Error())
	}
	defer mux.Close()
	go mux.Serve()

	rootPool := cryptox509.NewCertPool()
	if !rootPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatalf("failed to add CA cert to pool")
	}

	dial := func(certPEM, keyPEM []byte) (*tls.Conn, error) {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			t.Fatalf("failed to build keypair: %s", err)
		}
		return tls.Dial("tcp", tcpListener.Addr().String(), &tls.Config{
			RootCAs:      rootPool,
			Certificates: []tls.Certificate{cert},
			ServerName:   "127.0.0.1",
		})
	}

	// Allowed CommonName — handshake should succeed and the connection should be usable.
	conn, err := dial(allowedPEM, allowedKeyPEM)
	if err != nil {
		t.Fatalf("expected handshake with matching CommonName to succeed, got error: %s", err)
	}
	if err := conn.Handshake(); err != nil {
		t.Fatalf("explicit handshake with matching CommonName failed: %s", err)
	}
	conn.Close()

	// Denied CommonName — server must reject. With TLS 1.3 the client's Dial may return
	// nil; a subsequent Read surfaces the server's alert.
	conn, err = dial(deniedPEM, deniedKeyPEM)
	if err == nil {
		var b [1]byte
		_, err = conn.Read(b[:])
		conn.Close()
	}
	if err == nil {
		t.Fatalf("expected handshake with non-matching CommonName to fail, got nil")
	}
	if !strings.Contains(err.Error(), "bad certificate") && !strings.Contains(err.Error(), "did not provide any valid certificate that matches CommonName") {
		t.Fatalf("expected CCommonNameN mismatch error, got: %s", err.Error())
	}
}

type mockAddr struct {
	Nwk  string
	Addr string
}

func (m *mockAddr) Network() string {
	return m.Nwk
}

func (m *mockAddr) String() string {
	return m.Addr
}

// Test_MuxCloseConns verifies that CloseConns actually closes the underlying
// socket of a tracked connection (not merely drops it from the tracking set),
// so a peer parked in a read on it is unblocked. This is the property that lets
// a node shut down while parked reading a Raft snapshot body from a peer that
// has gone silent. See https://github.com/rqlite/rqlite/issues/2687.
func Test_MuxCloseConns(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	mux, err := NewMux(tcpListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	if !testing.Verbose() {
		mux.Logger = log.New(io.Discard, "", 0)
	}
	go mux.Serve()

	// Connecting is enough for the Mux to accept and track the connection.
	client, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial mux: %s", err.Error())
	}
	defer client.Close()
	if !waitForTrackedConns(mux, 1, 5*time.Second) {
		t.Fatalf("connection was not tracked, got %d", numTrackedConns(mux))
	}

	// Park a read on the connection; nothing is ever sent on it.
	readReturned := make(chan error, 1)
	go func() {
		var b [1]byte
		_, err := client.Read(b[:])
		readReturned <- err
	}()

	// Force-close all connections; the parked read must return promptly.
	if err := mux.CloseConns(); err != nil {
		t.Fatalf("CloseConns returned error: %s", err.Error())
	}
	select {
	case err := <-readReturned:
		if err == nil {
			t.Fatal("expected blocked read to return an error after CloseConns")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked read did not return after CloseConns")
	}

	// The tracking set must be empty again, so it does not grow without bound.
	if got := numTrackedConns(mux); got != 0 {
		t.Fatalf("expected 0 tracked connections after close, got %d", got)
	}
}

// Test_MuxConnDeregisterOnClose verifies that an accepted connection deregisters
// itself from the Mux's tracking set when closed, so the set does not leak over
// the lifetime of the node. Connecting is enough for the Mux to track the
// connection; no header byte is required.
func Test_MuxConnDeregisterOnClose(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()

	mux, err := NewMux(tcpListener, nil)
	if err != nil {
		t.Fatalf("failed to create mux: %s", err.Error())
	}
	if !testing.Verbose() {
		mux.Logger = log.New(io.Discard, "", 0)
	}
	go mux.Serve()

	// Connecting is enough for the Mux to accept and track the connection.
	client, err := net.Dial("tcp", tcpListener.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial mux: %s", err.Error())
	}
	if !waitForTrackedConns(mux, 1, 5*time.Second) {
		t.Fatalf("connection was not tracked after connecting, got %d", numTrackedConns(mux))
	}

	// Closing the connection must deregister it from the tracking set.
	client.Close()
	if !waitForTrackedConns(mux, 0, 5*time.Second) {
		t.Fatalf("connection was not deregistered after close, got %d", numTrackedConns(mux))
	}
}

// numTrackedConns returns the number of connections currently tracked by the
// Mux for force-close on shutdown.
func numTrackedConns(mux *Mux) int {
	mux.connsMu.Lock()
	defer mux.connsMu.Unlock()
	return len(mux.conns)
}

// waitForTrackedConns waits up to timeout for the Mux to be tracking exactly n
// connections, returning true if that count is reached.
func waitForTrackedConns(mux *Mux, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if numTrackedConns(mux) == n {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
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

func mustRename(new, old string) {
	if err := os.Rename(old, new); err != nil {
		panic(err)
	}
}
