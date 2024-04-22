package pool

import (
	"net"
	"sync"
)

// Conn is a wrapper around net.Conn to modify the behavior of
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

// MarkUnusable marks the connection not usable anymore, to let the pool close it instead of returning it to pool.
func (p *Conn) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}
