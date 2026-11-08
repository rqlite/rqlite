package servicetest

import (
	"net"
	"sync"
	"sync/atomic"
)

// Service represents a test service. Like the real cluster service it keeps
// a connection open and serves commands on it one after another: Handler is
// called once per command, and again on the same connection when it returns,
// until the connection fails or a Handler call finishes without using it.
type Service struct {
	Listener net.Listener
	Handler  func(net.Conn)

	mu    sync.Mutex
	conns []net.Conn
}

// NewService returns a new instance of the service that runs on
// a node, which responds to internode (Raft) communication. It is
// used to simulate a remote node in a cluster.
func NewService() *Service {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("service: failed to listen: " + err.Error())
	}
	return &Service{
		Listener: ln,
	}
}

// Start starts the service.
func (s *Service) Start() {
	go s.serve()
}

// Addr returns the address of the service.
func (s *Service) Addr() string {
	return s.Listener.Addr().String()
}

// Close closes the service and every connection it accepted.
func (s *Service) Close() error {
	err := s.Listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.conns = nil
	return err
}

func (s *Service) serve() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer conn.Close()
	if s.Handler == nil {
		return
	}
	tc := &trackedConn{Conn: conn}
	for {
		tc.used.Store(false)
		s.Handler(tc)
		if tc.failed.Load() || !tc.used.Load() {
			return
		}
	}
}

// trackedConn records whether a Handler used the connection and whether the
// connection has failed, so the service knows whether to serve another
// command on it.
type trackedConn struct {
	net.Conn
	used   atomic.Bool
	failed atomic.Bool
}

func (c *trackedConn) Read(b []byte) (int, error) {
	c.used.Store(true)
	n, err := c.Conn.Read(b)
	if err != nil {
		c.failed.Store(true)
	}
	return n, err
}

func (c *trackedConn) Write(b []byte) (int, error) {
	c.used.Store(true)
	n, err := c.Conn.Write(b)
	if err != nil {
		c.failed.Store(true)
	}
	return n, err
}

func (c *trackedConn) Close() error {
	c.failed.Store(true)
	return c.Conn.Close()
}
