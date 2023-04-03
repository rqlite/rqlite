package servicetest

import "net"

type Service struct {
	Listener net.Listener
	Handler  func(net.Conn)
}

func NewService() *Service {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("service: failed to listen: " + err.Error())
	}
	return &Service{
		Listener: ln,
	}
}

func (s *Service) Start() error {
	return s.serve()
}

func (s *Service) Addr() string {
	return s.Listener.Addr().String()
}

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
	conn.Close()
}
