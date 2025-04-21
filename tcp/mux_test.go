package tcp

import (
	"bytes"
	"crypto/tls"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/rqlite/rqlite/v8/testdata/x509"
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
		for i := uint8(0); i < n; i++ {
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

	mux, err := NewTLSMux(tcpListener, nil, cert, key, "", true, false)
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

	state := conn.ConnectionState()
	if !state.HandshakeComplete {
		t.Fatal("connection handshake failed to complete")
	}
	if state.PeerCertificates[0].Subject.CommonName != "example.com" {
		t.Fatalf("unexpected common name: %s", state.PeerCertificates[0].Subject.CommonName)
	}
}

func TestTLSMux_Fail(t *testing.T) {
	tcpListener := mustTCPListener("127.0.0.1:0")
	defer tcpListener.Close()
	_, err := NewTLSMux(tcpListener, nil, "xxxx", "yyyy", "", true, false)
	if err == nil {
		t.Fatalf("created mux unexpectedly with bad resources")
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

// mustTCPListener returns a listener on bind, or panics.
func mustTCPListener(bind string) net.Listener {
	l, err := net.Listen("tcp", bind)
	if err != nil {
		panic(err)
	}
	return l
}
