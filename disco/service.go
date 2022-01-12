package disco

import (
	"fmt"

	consul "github.com/rqlite/rqlite-disco-clients/consul"
	etcd "github.com/rqlite/rqlite-disco-clients/etcd"
)

type Client interface {
	GetLeader() (id string, apiAddr string, addr string, ok bool, e error)
	InitializeLeader(id, apiAddr, addr string) (bool, error)
	SetLeader(id, apiAddr, addr string) error
}

type Service struct {
	c Client
}

func New(t, key, cfgPath string) (*Service, error) {
	var c Client

	if t == "consul" {
		cfg, err := consul.NewConfigFromFile(cfgPath)
		if err != nil {
			return nil, err
		}

		c, err = consul.New(key, cfg)
		if err != nil {
			return nil, err
		}
	} else if t == "etcd" {
		cfg, err := etcd.NewConfigFromFile(cfgPath)
		if err != nil {
			return nil, err
		}

		c, err = etcd.New(key, cfg)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("invalid disco service: %s", t)
	}

	return &Service{
		c: c,
	}, nil
}

func (s *Service) Run() {
	for {
		time.Sleep(5 * time.Second)
		id, apiAddr, addr, ok, err := s.c.GetLeader()
		if err != nil {
			continue
		}

		if !ok {

		}
	}
}
