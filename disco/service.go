package disco

import (
	"log"
	"os"
)

type Client interface {
	GetLeader() (id string, apiAddr string, addr string, ok bool, e error)
	InitializeLeader(id, apiAddr, addr string) (bool, error)
	SetLeader(id, apiAddr, addr string) error
}

type Service struct {
	c Client

	logger *log.Logger
}

func NewService(c Client) *Service {
	return &Service{
		c:      c,
		logger: log.New(os.Stderr, "[disco] ", log.LstdFlags),
	}
}

func (s *Service) Register() error {
	return nil
}

func (s *Service) StartReporting() {

}
