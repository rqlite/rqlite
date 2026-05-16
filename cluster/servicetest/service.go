package servicetest

import (
	"net"
)

// Service represents a test service.
type Service struct {
	Listener net.Listener
	Handler  func(net.Conn)
	// CloseConn controls whether the service closes a connection after Handler returns.
	CloseConn bool
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
		Listener:  ln,
		CloseConn: true,
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

// Close closes the service.
func (s *Service) Close() error {
	return s.Listener.Close()
}

func (s *Service) serve() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	if s.Handler != nil {
		s.Handler(conn)
	}
	if s.CloseConn {
		conn.Close()
	}
}
