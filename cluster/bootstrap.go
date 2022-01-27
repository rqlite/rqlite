package cluster

import (
	"crypto/tls"
	"log"
	"os"
	"time"
)

type AddressProvider interface {
	Lookup() ([]string, error)
}

type Bootstrapper struct {
	provider     AddressProvider
	expect       int
	joinAttempts int
	joinInterval time.Duration
	tlsConfig    *tls.Config

	logger *log.Logger
}

func NewBootstrapper(p AddressProvider, expect int, joinAttempts int, joinInterval time.Duration,
	tlsConfig *tls.Config) *Bootstrapper {
	return &Bootstrapper{
		provider:     p,
		expect:       expect,
		joinAttempts: joinAttempts,
		joinInterval: joinInterval,
		tlsConfig:    tlsConfig,
		logger:       log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
	}
}

func (b *Bootstrapper) Boot(id, raftAddr string) error {
	targets, err := b.provider.Lookup()
	if err != nil {
		return err
	}
	// Check len of targets, don't do anything until at expect level.
	return Notify(targets, id, raftAddr, b.joinAttempts, b.joinInterval, b.tlsConfig)
	// Really, if this fails, try a normal join. XXX
}

type stringAddressProvider struct {
	ss []string
}

func (s *stringAddressProvider) Lookup() ([]string, error) {
	return s.ss, nil
}

func NewAddressProviderString(ss []string) AddressProvider {
	return &stringAddressProvider{ss}
}
