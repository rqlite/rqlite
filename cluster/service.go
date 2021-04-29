package cluster

import (
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

// stats captures stats for the Cluster service.
var stats *expvar.Map

const (
	numGetNodeAPI = "num_get_node_api"
)

func init() {
	stats = expvar.NewMap("cluster")
	stats.Add(numGetNodeAPI, 0)
}

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
	https   bool   // Serving HTTPS?
	apiAddr string // host:port this node serves the HTTP API.

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
	go s.serve()
	s.logger.Println("service listening on", s.tn.Addr())
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.tn.Close()
	return nil
}

// Addr returns the address the service is listening on.
func (s *Service) Addr() string {
	return s.addr.String()
}

func (s *Service) EnableHTTPS(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.https = b
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

	// Send the request
	c := &Command{
		Type: Command_COMMAND_TYPE_GET_NODE_API_URL,
	}
	p, err := proto.Marshal(c)
	if err != nil {
		conn.Close()
	}

	// Write length of Protobuf, the Protobuf
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b[0:], uint16(len(p)))

	conn.Write(b)
	conn.Write(p)

	b, err = ioutil.ReadAll(conn)
	if err != nil {
		return "", err
	}

	a := &Address{}
	err = proto.Unmarshal(b, a)
	if err != nil {
		return "", err
	}

	return a.Url, nil
}

// Stats returns status of the Service.
func (s *Service) Stats() (map[string]interface{}, error) {
	st := map[string]interface{}{
		"addr":     s.addr.String(),
		"timeout":  s.timeout.String(),
		"https":    strconv.FormatBool(s.https),
		"api_addr": s.apiAddr,
	}

	return st, nil
}

func (s *Service) serve() error {
	for {
		conn, err := s.tn.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	defer conn.Close()

	b := make([]byte, 4)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		return
	}
	sz := binary.LittleEndian.Uint16(b[0:])

	b = make([]byte, sz)
	_, err = io.ReadFull(conn, b)
	if err != nil {
		return
	}

	c := &Command{}
	err = proto.Unmarshal(b, c)
	if err != nil {
		conn.Close()
	}

	switch c.Type {
	case Command_COMMAND_TYPE_GET_NODE_API_URL:
		stats.Add(numGetNodeAPI, 1)

		s.mu.RLock()
		defer s.mu.RUnlock()

		a := &Address{}
		scheme := "http"
		if s.https {
			scheme = "https"
		}
		a.Url = fmt.Sprintf("%s://%s", scheme, s.apiAddr)

		b, err = proto.Marshal(a)
		if err != nil {
			conn.Close()
		}
		conn.Write(b)
	}
}
