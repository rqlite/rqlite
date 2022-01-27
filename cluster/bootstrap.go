package cluster

import (
	"crypto/tls"
	"log"
	"net/http"
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
	bs := &Bootstrapper{
		provider:     p,
		expect:       expect,
		joinAttempts: joinAttempts,
		joinInterval: joinInterval,
		tlsConfig:    &tls.Config{InsecureSkipVerify: true},
		logger:       log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
	}
	if tlsConfig != nil {
		bs.tlsConfig = tlsConfig
	}
	return bs
}

func (b *Bootstrapper) Boot(id, raftAddr string) error {
	targets, err := b.provider.Lookup()
	if err != nil {
		return err
	}
	// Check len of targets, don't do anything until at expect level.
	return b.notify(targets, id, raftAddr)
	// Really, if this fails, try a normal join. XXX
}

func (b *Bootstrapper) notify(targets []string, id, raftAddr string) error {
	// Create and configure the client to connect to the other node.
	tr := &http.Transport{
		TLSClientConfig: b.tlsConfig,
	}
	client := &http.Client{Transport: tr}
	_ = client
	return nil
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
