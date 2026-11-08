//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package pool

import (
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"
)

func socketPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func awaitClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !connClosed(conn) {
		if time.Now().After(deadline) {
			t.Fatal("peer closure not detected")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestConnClosed verifies TCP and TLS socket checks preserve live connections.
func TestConnClosed(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "tcp"
		if encrypted {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			client, server := socketPair(t)
			if encrypted {
				client = tls.Client(client, &tls.Config{})
			}
			if connClosed(client) {
				t.Fatal("live connection reported closed")
			}
			server.Close()
			awaitClosed(t, client)
		})
	}
}

// TestConnClosedPreservesData verifies peeking never consumes pending bytes.
func TestConnClosedPreservesData(t *testing.T) {
	client, server := socketPair(t)
	if _, err := server.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if connClosed(client) {
		t.Fatal("connection with data reported closed")
	}
	client.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := io.ReadFull(client, b[:]); err != nil || b[0] != 'x' {
		t.Fatalf("read: %q, %v", b, err)
	}
}

// TestPoolSkipsClosed verifies a stale pool is replaced without application writes.
func TestPoolSkipsClosed(t *testing.T) {
	fresh, _ := socketPair(t)
	dials := 0
	p, err := NewChannelPool(8, func() (net.Conn, error) { dials++; return fresh, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	cp := p.(*channelPool)
	for range 8 {
		client, server := socketPair(t)
		server.Close()
		awaitClosed(t, client)
		cp.conns <- client
		cp.nOpenConns++
	}
	conn, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if dials != 1 || cp.nOpenConns != 1 || p.Len() != 0 {
		t.Fatalf("dials=%d open=%d idle=%d", dials, cp.nOpenConns, p.Len())
	}
	conn.Close()
	conn, err = p.Get()
	if err != nil || dials != 1 {
		t.Fatalf("healthy connection not reused: dials=%d error=%v", dials, err)
	}
	conn.Close()
}

// TestPoolKeepsHealthy verifies a stale peer does not invalidate other idle sockets.
func TestPoolKeepsHealthy(t *testing.T) {
	p, _ := NewChannelPool(3, func() (net.Conn, error) { t.Fatal("unexpected dial"); return nil, nil })
	defer p.Close()
	cp := p.(*channelPool)
	stale, peer := socketPair(t)
	peer.Close()
	awaitClosed(t, stale)
	healthy, _ := socketPair(t)
	other, _ := socketPair(t)
	cp.conns <- stale
	cp.conns <- healthy
	cp.conns <- other
	cp.nOpenConns = 3
	conn, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if conn.(*Conn).Conn != healthy || p.Len() != 1 || cp.nOpenConns != 2 {
		t.Fatal("healthy idle connections were not preserved")
	}
	conn.Close()
}

// TestConnClosedFallback verifies non-socket connections are retained.
func TestConnClosedFallback(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if connClosed(a) {
		t.Fatal("non-socket connection rejected")
	}
}
