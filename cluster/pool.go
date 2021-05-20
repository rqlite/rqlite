package cluster

import (
	"net"
	"sync"
	"time"
)

type PoolConn struct {
	net.Conn
	conn net.Conn
	pool *PooledTransport
}

func (pc *PoolConn) Write(b []byte) (n int, err error) {
	return pc.conn.Write(b)
}

func (pc *PoolConn) Read(b []byte) (n int, err error) {
	return pc.conn.Read(b)
}

func (pc *PoolConn) Close() error {
	return pc.conn.Close()
}

type PooledTransport struct {
	mu    sync.RWMutex
	tn    Transport
	conns map[string]chan net.Conn
}

func NewPooledTransport(tn Transport) *PooledTransport {
	c := &PooledTransport{
		tn: tn,
	}

	return c
}

func (pt *PooledTransport) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return pt.tn.Dial(address, timeout)
}

func (pt *PooledTransport) Accept() (net.Conn, error) {
	return pt.tn.Accept()
}

func (pt *PooledTransport) Close() error {
	return pt.tn.Close()
}

func (pt *PooledTransport) Addr() net.Addr {
	return pt.tn.Addr()
}
