package cluster

import (
	"errors"
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
	pc.pool.mu.Lock()
	defer pc.pool.mu.Unlock()
	return pc.conn.Close()
}

type PooledTransport struct {
	mu   sync.RWMutex
	tn   Transport
	pool map[string]chan net.Conn
}

// XXX This model isn't going to work. Too complex. I need a pool *per address*!!!
// Pool logic shouldn't care about address, just hold connections.
func NewPooledTransport(tn Transport) *PooledTransport {
	c := &PooledTransport{
		tn: tn,
	}

	return c
}

func (pt *PooledTransport) Dial(address string, timeout time.Duration) (net.Conn, error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	conns, ok := pt.pool[address]
	if ok {
		select {
		case conn := <-conns:
			if conn == nil {
				return nil, errors.New("connection pool closed")
			}

			return pt.wrapConn(conn), nil
		default:
			conn, err := pt.tn.Dial(address, timeout)
			if err != nil {
				return nil, err
			}

			return pt.wrapConn(conn), nil
		}
	}
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

func (pt *PooledTransport) wrapConn(c net.Conn) net.Conn {
	return &PoolConn{
		conn: c,
		pool: pt,
	}
}

func (pt *PooledTransport) returnConn()
