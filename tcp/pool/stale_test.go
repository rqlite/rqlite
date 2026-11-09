package pool

import (
	"crypto/tls"
	"crypto/x509/pkix"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/rtls"
)

// watchedServer accepts connections on loopback and hands each accepted
// connection to the test, so the test can act on the server side of a pooled
// connection and know when it has.
type watchedServer struct {
	ln       net.Listener
	accepted chan net.Conn
}

func newWatchedServer(t *testing.T, ln net.Listener) *watchedServer {
	t.Helper()
	s := &watchedServer{ln: ln, accepted: make(chan net.Conn, 16)}
	go func() {
		for {
			conn, err := s.ln.Accept()
			if err != nil {
				return
			}
			go func() {
				// A TLS server only handshakes when driven, and the test
				// goroutine is busy dialing, so drive it here.
				if tc, ok := conn.(*tls.Conn); ok {
					if err := tc.Handshake(); err != nil {
						conn.Close()
						return
					}
				}
				s.accepted <- conn
			}()
		}
	}()
	t.Cleanup(func() { s.ln.Close() })
	return s
}

func newTCPWatchedServer(t *testing.T) *watchedServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return newWatchedServer(t, ln)
}

func (s *watchedServer) factory() Factory {
	return func() (net.Conn, error) { return net.Dial("tcp", s.ln.Addr().String()) }
}

// next returns the next accepted connection, failing the test if none arrives.
func (s *watchedServer) next(t *testing.T) net.Conn {
	t.Helper()
	select {
	case conn := <-s.accepted:
		return conn
	case <-time.After(5 * time.Second):
		t.Fatal("server accepted no connection")
	}
	return nil
}

// deadNotifier returns a channel that receives each time one of p's watchers
// finds its idle connection unusable.
func deadNotifier(p Pool) <-chan struct{} {
	ch := make(chan struct{}, 16)
	p.(*channelPool).deadHook = func() { ch <- struct{}{} }
	return ch
}

// mustExchange checks that a byte written by the client arrives at the server.
func mustExchange(t *testing.T, client, server net.Conn) {
	t.Helper()
	if _, err := client.Write([]byte{'x'}); err != nil {
		t.Fatalf("client write: %s", err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(server, b); err != nil {
		t.Fatalf("server read: %s", err)
	}
}

// Test_Get_SkipsIdleConnClosedByPeer checks that a pooled connection whose
// server side has been closed is discarded by Get, which hands out a fresh one.
func Test_Get_SkipsIdleConnClosedByPeer(t *testing.T) {
	srv := newTCPWatchedServer(t)
	p, err := NewChannelPool(4, srv.factory())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	dead := deadNotifier(p)

	conn, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	serverSide := srv.next(t)
	conn.Close() // back to the pool
	if p.Len() != 1 {
		t.Fatalf("expected 1 idle conn, got %d", p.Len())
	}

	// The remote end goes away while the connection is idle.
	serverSide.Close()
	<-dead

	conn, err = p.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustExchange(t, conn, srv.next(t))

	if p.Len() != 0 {
		t.Fatalf("expected stale conn to be dropped from pool, idle count is %d", p.Len())
	}
	stats, _ := p.Stats()
	if exp, got := int64(1), stats["stale_discarded"]; exp != got {
		t.Fatalf("expected %d stale conn discarded, got %v", exp, got)
	}
	if exp, got := int64(1), stats["open_connections"]; exp != got {
		t.Fatalf("expected %d open conns, got %v", exp, got)
	}
}

// Test_Get_SkipsIdleConnWithUnexpectedData checks that a pooled connection
// which received bytes while idle is discarded rather than handed out.
func Test_Get_SkipsIdleConnWithUnexpectedData(t *testing.T) {
	srv := newTCPWatchedServer(t)
	p, err := NewChannelPool(4, srv.factory())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	dead := deadNotifier(p)

	conn, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	serverSide := srv.next(t)
	conn.Close()

	if _, err := serverSide.Write([]byte("unexpected")); err != nil {
		t.Fatal(err)
	}
	<-dead

	conn, err = p.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustExchange(t, conn, srv.next(t))
	stats, _ := p.Stats()
	if exp, got := int64(1), stats["stale_discarded"]; exp != got {
		t.Fatalf("expected %d stale conn discarded, got %v", exp, got)
	}
}

// Test_Get_ReusesIdleConnWithExpiredDeadline checks that a connection returned
// to the pool with an expired read deadline, as every connection the cluster
// client returns has, is still reused and is fully usable afterwards.
func Test_Get_ReusesIdleConnWithExpiredDeadline(t *testing.T) {
	srv := newTCPWatchedServer(t)
	p, err := NewChannelPool(4, srv.factory())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	conn, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	serverSide := srv.next(t)
	conn.SetReadDeadline(aLongTimeAgo)
	conn.Close()

	conn, err = p.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustExchange(t, conn, serverSide) // The same connection, not a new one.

	// The pool left no deadline behind: a read waits for data.
	if _, err := serverSide.Write([]byte{'y'}); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(conn, b); err != nil {
		t.Fatalf("client read on reused conn: %s", err)
	}
	stats, _ := p.Stats()
	if exp, got := int64(0), stats["stale_discarded"]; exp != got {
		t.Fatalf("expected %d stale conns discarded, got %v", exp, got)
	}
}

// Test_Get_SkipsIdleTLSConnClosedByPeer is the closed-by-peer case for a TLS
// connection, where the server's Close sends a close_notify alert before the
// connection ends.
func Test_Get_SkipsIdleTLSConnClosedByPeer(t *testing.T) {
	certPEM, keyPEM, err := rtls.GenerateSelfSignedCertIPSAN(pkix.Name{CommonName: "localhost"},
		time.Hour, 2048, net.ParseIP("127.0.0.1"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	srv := newWatchedServer(t, ln)
	factory := func() (net.Conn, error) {
		return tls.Dial("tcp", ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	}
	p, err := NewChannelPool(4, factory)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	dead := deadNotifier(p)

	conn, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	serverSide := srv.next(t)
	mustExchange(t, conn, serverSide)
	conn.Close()

	serverSide.Close()
	<-dead

	conn, err = p.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustExchange(t, conn, srv.next(t))
	stats, _ := p.Stats()
	if exp, got := int64(1), stats["stale_discarded"]; exp != got {
		t.Fatalf("expected %d stale conn discarded, got %v", exp, got)
	}
}

// Benchmark_GetClose measures the cost of taking an idle connection from the
// pool and returning it, which is the per-request cost of watching it.
func Benchmark_GetClose(b *testing.B) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()
	p, err := NewChannelPool(4, func() (net.Conn, error) { return net.Dial("tcp", ln.Addr().String()) })
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	conn, err := p.Get()
	if err != nil {
		b.Fatal(err)
	}
	conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := p.Get()
		if err != nil {
			b.Fatal(err)
		}
		conn.Close()
	}
	b.StopTimer()
	if stats, _ := p.Stats(); stats["stale_discarded"] != int64(0) {
		b.Fatalf("unexpected stale connections: %v", stats["stale_discarded"])
	}
}
