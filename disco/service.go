package disco

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Client is the interface discovery clients must implement.
type Client interface {
	GetLeader() (id string, apiAddr string, addr string, ok bool, e error)
	InitializeLeader(id, apiAddr, addr string) (bool, error)
	SetLeader(id, apiAddr, addr string) error
	fmt.Stringer
}

// Store is the interface the consensus system must implement.
type Store interface {
	IsLeader() bool
	RegisterLeaderChange(c chan<- struct{})
}

// Service represents a Discovery Service instance.
type Service struct {
	UpdateInterval time.Duration

	c Client
	s Store

	logger *log.Logger

	mu          sync.Mutex
	lastContact time.Time
}

// NewService returns an instantiated Discovery Service.
func NewService(c Client, s Store) *Service {
	return &Service{
		c: c,
		s: s,

		UpdateInterval: 5 * time.Second,
		logger:         log.New(os.Stderr, "[disco] ", log.LstdFlags),
	}
}

// Register registers this node with the discovery service. It will block
// until a) the node registers itself as leader, b) learns of another node
// it can use to join the cluster, or c) an unrecoverable error occurs.
func (s *Service) Register(id, apiAddr, addr string) (bool, string, error) {
	for {
		_, cAPIAddr, _, ok, err := s.c.GetLeader()
		if err != nil {
			s.logger.Printf("failed to get leader: %s", err.Error())
		}
		if ok {
			s.updateContact(time.Now())
			return false, cAPIAddr, nil
		}

		ok, err = s.c.InitializeLeader(id, apiAddr, addr)
		if err != nil {
			s.logger.Printf("failed to initialize as Leader: %s", err.Error())
		}
		if ok {
			s.updateContact(time.Now())
			return true, apiAddr, nil
		}

		time.Sleep(s.UpdateInterval)
	}
}

// StartReporting reports the details of this node to the discovery service,
// if, and only if, this node is the leader. The service will report
// anytime a leadership change is detected. It also does it periodically
// to deal with any intermittent issues that caused Leadership information
/// to go stale.
func (s *Service) StartReporting(id, apiAddr, addr string) {
	ticker := time.NewTicker(6 * s.UpdateInterval)
	obCh := make(chan struct{})
	s.s.RegisterLeaderChange(obCh)

	update := func(changed bool) {
		if s.s.IsLeader() {
			if err := s.c.SetLeader(id, apiAddr, addr); err != nil {
				s.logger.Printf("failed to update discovery service with Leader details: %s",
					err.Error())
			}
			if changed {
				s.logger.Println("updated Leader due to leadership change")
			}
			s.updateContact(time.Now())
		}
	}

	for {
		select {
		case <-ticker.C:
			update(false)
		case <-obCh:
			update(true)
		}
	}
}

// Stats returns diagnostic information on the disco service.
func (s *Service) Stats() (map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]interface{}{
		"name":            s.c.String(),
		"update_interval": s.UpdateInterval,
		"last_contact":    s.lastContact,
	}, nil
}

func (s *Service) updateContact(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastContact = t
}
