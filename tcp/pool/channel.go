package pool

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.RWMutex
	conns chan net.Conn

	// net.Conn generator
	factory    Factory
	nOpenConns int64
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewChannelPool returns a new pool based on buffered channels with a maximum capacity.
// During a Get(), If there is no new connection available in the pool, a new connection
// will be created via the Factory() method.
func NewChannelPool(maxCap int, factory Factory) (Pool, error) {
	return &channelPool{
		conns:   make(chan net.Conn, maxCap),
		factory: factory,
	}, nil
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get() (net.Conn, error) {
	conns, factory := c.getConnsAndFactory()
	if conns == nil {
		return nil, ErrClosed
	}

	// Wrap our connections with our custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return c.wrapConn(conn), nil
	default:
		conn, err := factory()
		if err != nil {
			return nil, err
		}
		atomic.AddInt64(&c.nOpenConns, 1)
		return c.wrapConn(conn), nil
	}
}

// Close closes every connection in the pool.
func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
	atomic.StoreInt64(&c.nOpenConns, 0)
}

// Len returns the number of idle connections.
func (c *channelPool) Len() int {
	conns, _ := c.getConnsAndFactory()
	return len(conns)
}

// Stats returns stats for the pool.
func (c *channelPool) Stats() (map[string]interface{}, error) {
	conns, _ := c.getConnsAndFactory()
	return map[string]interface{}{
		"idle":                 len(conns),
		"open_connections":     c.nOpenConns,
		"max_open_connections": cap(conns),
	}, nil
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		atomic.AddInt64(&c.nOpenConns, -1)
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		atomic.AddInt64(&c.nOpenConns, -1)
		return conn.Close()
	}
}

func (c *channelPool) getConnsAndFactory() (chan net.Conn, Factory) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conns, c.factory
}

// wrapConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
	p := &Conn{c: c}
	p.Conn = conn
	return p
}
