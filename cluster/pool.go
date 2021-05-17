package cluster

import (
	"errors"
	"net"
	"sync"
)

type Pool struct {
	mu    sync.RWMutex
	conns chan net.Conn
}

type PoolConn struct {
	net.Conn // XXXX how does this work?
	conn     net.Conn
	pool     *Pool
}

func (pc *PoolConn) Close() error {
	// XXX add back to pool
	return pc.conn.Close()
}

func NewPool(initialCap, maxCap int) (*Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &Pool{
		conns: make(chan net.Conn, maxCap),
	}

	return c, nil
}

func (p *Pool) WrapConn(c net.Conn) net.Conn {
	return &PoolConn{
		conn: c,
		pool: p,
	}
}
