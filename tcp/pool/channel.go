package pool

import (
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// aLongTimeAgo is a non-zero time far in the past. Setting it as a read
// deadline wakes a blocked Read with a timeout error at once.
var aLongTimeAgo = time.Unix(1, 0)

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.RWMutex
	conns chan *idleConn

	// net.Conn generator
	factory    Factory
	nOpenConns atomic.Int64
	nStale     atomic.Int64

	// deadHook, if set, is called by a watcher that has found its idle
	// connection unusable. Tests use it to wait for that moment.
	deadHook func()
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// NewChannelPool returns a new pool based on buffered channels with a maximum capacity.
// During a Get(), if there is no new connection available in the pool, a new connection
// will be created via the Factory() method.
func NewChannelPool(maxCap int, factory Factory) (Pool, error) {
	if factory == nil {
		return nil, errors.New("factory is nil")
	}
	if maxCap <= 0 {
		return nil, errors.New("invalid capacity settings")
	}
	return &channelPool{
		conns:   make(chan *idleConn, maxCap),
		factory: factory,
	}, nil
}

// Get implements the Pool interface's Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method. Do not call Get() on a closed pool.
//
// An idle connection whose remote side has gone away is closed and skipped,
// so a connection returned by Get is one the pool has no reason to doubt.
func (c *channelPool) Get() (net.Conn, error) {
	conns, factory := c.getConnsAndFactory()
	if conns == nil {
		return nil, ErrClosed
	}

	// Wrap our connections with our custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	for {
		select {
		case ic := <-conns:
			if ic == nil {
				return nil, ErrClosed
			}
			conn, ok := ic.activate()
			if !ok {
				c.discard(conn)
				continue
			}
			return c.wrapConn(conn), nil
		default:
			conn, err := factory()
			if err != nil {
				return nil, err
			}
			c.nOpenConns.Add(1)
			return c.wrapConn(conn), nil
		}
	}
}

// Close closes every connection in the pool.
func (c *channelPool) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	conns := c.conns
	c.conns = nil
	c.factory = nil

	if conns == nil {
		return
	}

	close(conns)
	for ic := range conns {
		ic.Close()
	}
	c.nOpenConns.Store(0)
}

// Len returns the number of idle connections.
func (c *channelPool) Len() int {
	conns, _ := c.getConnsAndFactory()
	return len(conns)
}

// Stats returns stats for the pool.
func (c *channelPool) Stats() (map[string]any, error) {
	conns, _ := c.getConnsAndFactory()
	return map[string]any{
		"idle":                 len(conns),
		"open_connections":     c.nOpenConns.Load(),
		"max_open_connections": cap(conns),
		"stale_discarded":      c.nStale.Load(),
	}, nil
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Only put adds to the channel, and put holds the lock, so a channel with
	// room now still has room below. A closed or full pool closes the
	// connection instead.
	if c.conns == nil || len(c.conns) == cap(c.conns) {
		return c.discard(conn)
	}
	c.conns <- newIdleConn(conn, c.connDead)
	return nil
}

// discard closes a connection the pool opened and will not hand out again.
func (c *channelPool) discard(conn net.Conn) error {
	c.nOpenConns.Add(-1)
	return conn.Close()
}

// connDead records that a watcher found an idle connection unusable.
func (c *channelPool) connDead() {
	c.nStale.Add(1)
	if c.deadHook != nil {
		c.deadHook()
	}
}

func (c *channelPool) getConnsAndFactory() (chan *idleConn, Factory) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conns, c.factory
}

// wrapConn wraps a standard net.Conn in a pool Conn.
func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
	p := &Conn{c: c}
	p.Conn = conn
	return p
}

// idleConn is a connection sitting in the pool. While it is idle a watcher
// goroutine blocks reading from it. Nothing is expected to arrive on an idle
// connection, so that read only returns when the remote side has closed it,
// reset it, or sent something unexpected -- in every case the connection must
// not be handed out again. The watcher is stopped, and its verdict read, when
// the connection is activated for use.
//
// The watcher learns of the remote side going away when the network poller
// delivers the event, so a connection that dies in the instant before it is
// taken from the pool can still be handed out. A connection that has been dead
// for longer than that -- the remote node restarted, or closed it as idle -- is
// never handed out.
type idleConn struct {
	net.Conn
	onDead func()
	done   chan struct{} // closed when the watcher exits
	dead   atomic.Bool   // set by the watcher when the connection is unusable
}

// newIdleConn starts watching conn. Any read deadline left armed by the
// previous user is cleared first, so the watcher blocks instead of timing out.
// onDead is called if the watcher finds the connection unusable.
func newIdleConn(conn net.Conn, onDead func()) *idleConn {
	conn.SetReadDeadline(time.Time{})
	ic := &idleConn{
		Conn:   conn,
		onDead: onDead,
		done:   make(chan struct{}),
	}
	go ic.watch()
	return ic
}

// watch blocks reading from the connection until it is activated or the
// connection proves unusable. Reading through the connection, rather than
// peeking at the socket beneath it, lets a TLS connection consume the
// post-handshake messages a server sends while idle, such as session tickets,
// which are not data and do not make the connection unusable.
func (ic *idleConn) watch() {
	defer close(ic.done)
	var b [1]byte
	_, err := ic.Conn.Read(b[:])
	if errors.Is(err, os.ErrDeadlineExceeded) {
		// Only the pool touches an idle connection, and it clears the read
		// deadline before the watcher starts, so this is activate waking us.
		return
	}
	// Data, EOF, reset, or any other error: the connection is done for.
	ic.dead.Store(true)
	if ic.onDead != nil {
		ic.onDead()
	}
}

// activate stops the watcher and returns the connection for use, reporting
// whether it is still usable.
func (ic *idleConn) activate() (net.Conn, bool) {
	ic.Conn.SetReadDeadline(aLongTimeAgo)
	<-ic.done
	if ic.dead.Load() {
		return ic.Conn, false
	}
	ic.Conn.SetReadDeadline(time.Time{})
	return ic.Conn, true
}

// Close closes the connection and waits for its watcher to exit.
func (ic *idleConn) Close() error {
	err := ic.Conn.Close()
	<-ic.done
	return err
}
