package disco

import (
	"log"
	"os"
	"time"
)

type Client interface {
	GetLeader() (id string, apiAddr string, addr string, ok bool, e error)
	InitializeLeader(id, apiAddr, addr string) (bool, error)
	SetLeader(id, apiAddr, addr string) error
}

type Service struct {
	Delay time.Duration

	c      Client
	logger *log.Logger
}

func NewService(c Client) *Service {
	return &Service{
		Delay:  5 * time.Second,
		c:      c,
		logger: log.New(os.Stderr, "[disco] ", log.LstdFlags),
	}
}

// Register registers this node with the discovery service. It will block
// until a) the node registers itself as leader, b) learns of another node
// it can use to join the cluster, or c) an unrecoverable error occurs.
func (s *Service) Register(id, apiAddr, addr string) (bool, string, error) {
	for {
		cID, cAPIAddr, cAddr, ok, err := s.c.GetLeader()
		if err != nil {
			s.logger.Printf("failed to get leader: %s", err.Error())
		}
		if ok {
			s.logger.Printf("node %s (Raft address %s) detected node at %s as Leader",
				cID, cAddr, cAPIAddr)
			return false, cAPIAddr, nil
		}

		ok, err = s.c.InitializeLeader(id, apiAddr, addr)
		if err != nil {
			s.logger.Printf("failed to initialize as Leader: %s", err.Error())
		}
		if ok {
			s.logger.Printf("node %s registered as Leader", id)
			return true, apiAddr, nil
		}

		time.Sleep(s.Delay)
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
		}
		time.Sleep(s.Delay * 2)
	}
}
