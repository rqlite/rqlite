package cluster

import (
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// Transport is the interface the network layer must provide.
type Transport interface {
	net.Listener

	// Dial is used to create a connection to a service listening
	// on an address.
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Service provides information about the node and cluster.
type Service struct {
	tn      Transport // Network layer this service uses
	addr    net.Addr  // Address on which this service is listening
	timeout time.Duration

	mu      sync.RWMutex
	apiAddr string // host:port this node serves the HTTP API.

	wg sync.WaitGroup

	logger *log.Logger
}

// NewService returns a new instance of the cluster service
func NewService(tn Transport) *Service {
	return &Service{
		tn:      tn,
		addr:    tn.Addr(),
		timeout: 10 * time.Second,
		logger:  log.New(os.Stderr, "[cluster] ", log.LstdFlags),
	}
}

// Open opens the Service.
func (s *Service) Open() error {
	s.wg.Add(1)
	go s.serve()
	s.logger.Println("service listening on", s.tn.Addr())
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.tn.Close()
	s.wg.Wait()
	return nil
}

// Addr returns the address the service is listening on.
func (s *Service) Addr() string {
	return s.addr.String()
}

func (s *Service) SetAPIAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apiAddr = addr
}

func (s *Service) GetAPIAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.apiAddr
}

func (s *Service) GetNodeAPIAddr(nodeAddr string) (string, error) {
	conn, err := s.tn.Dial(nodeAddr, s.timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	b, err := ioutil.ReadAll(conn)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *Service) serve() error {
	defer s.wg.Done()
	for {
		conn, err := s.tn.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	// This is where we'd actually switch on incoming command in protobuf format
	// and write a protobuf back out.
	conn.Write([]byte(s.apiAddr))
	conn.Close()
}
