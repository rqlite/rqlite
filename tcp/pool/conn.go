package pool

import (
	"net"
	"sync"
)

// Conn is a wrapper around net.Conn to modify the the behavior of
// net.Conn's Close() method.
type Conn struct {
	net.Conn
	mu       sync.RWMutex
	c        *channelPool
	unusable bool
}

// Close puts the given connection back into the pool instead of closing it.
func (p *Conn) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		if p.Conn != nil {
			return p.Conn.Close()
		}
		return nil
	}
	return p.c.put(p.Conn)
}

// MarkUnusable marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *Conn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
	p := &Conn{c: c}
	p.Conn = conn
	return p
}
