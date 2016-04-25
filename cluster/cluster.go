package cluster

import (
	"encoding/json"
	"log"
	"net"
	"time"
)

const (
	ConnectionTimeout = 10 * time.Second
)

// Listener is the interface the network service must provide.
type Listener interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Store represents a store of information, managed via consensus.
type Store interface {
	// Leader returns the leader of the consensus system.
	Leader() string

	// UpdateAPIPeers updates the API peers on the store.
	UpdateAPIPeers(peers map[string]string) error
}

// Service allows access to the cluster and associated meta data,
// via consensus.
type Service struct {
	ln    Listener
	store Store

	logger *log.Logger
}

// NewService returns a new instance of the cluster service
func NewService(ln Listener, store Store) *Service {
	return &Service{
		ln:    ln,
		store: store,
	}
}

// Open opens the Service.
func (s *Service) Open() error {
	go s.serve()
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.ln.Close()
	return nil
}

// SetPeers will set the mapping between raftAddr and apiAddr for the entire cluster.
func (s *Service) SetPeers(raftAddr, apiAddr string) error {
	peer := map[string]string{
		raftAddr: apiAddr,
	}

	// Try the local store. It might be the leader.
	err := s.store.UpdateAPIPeers(peer)
	if err == nil {
		// All done! Aren't we lucky?
		return nil
	}

	// Try talking to the leader over the network.
	conn, err := s.ln.Dial(s.store.Leader(), ConnectionTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	b, err := json.Marshal(peer)
	if err != nil {
		return err
	}

	if _, err := conn.Write(b); err != nil {
		return err
	}

	// XXXX Wait for response and check for error.
	return nil
}

func (s *Service) serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) handleConn(conn net.Conn) {
	// Only handles peers updates for now.
	peers := make(map[string]string)
	d := json.NewDecoder(conn)

	err := d.Decode(&peers)
	if err != nil {
		return
	}

	// Update the peers.
	if err := s.store.UpdateAPIPeers(peers); err != nil {
		// Write error back down conn
		return
	}
	// write OK back down conn
}
