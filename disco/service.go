package disco

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Client interface {
	GetLeader() (id string, apiAddr string, addr string, ok bool, e error)
	InitializeLeader(id, apiAddr, addr string) (bool, error)
	SetLeader(id, apiAddr, addr string) error
	fmt.Stringer
}

// Service represents a Discovery Service instance.
type Service struct {
	UpdateInterval time.Duration

	c      Client
	logger *log.Logger

	mu          sync.Mutex
	lastContact time.Time
}

// NewService returns an instantiated Discovery Service.
func NewService(c Client) *Service {
	return &Service{
		UpdateInterval: 5 * time.Second,
		c:              c,
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
// if, and only if, this node is the leader.
func (s *Service) StartReporting(id, apiAddr, addr string, f func() bool) {
	for {
		if f() {
			if err := s.c.SetLeader(id, apiAddr, addr); err != nil {
				s.logger.Printf("failed to update discovery service with Leader details: %s",
					err.Error())
			}
			s.updateContact(time.Now())
		}
		time.Sleep(s.UpdateInterval * 2)
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
