package cluster

import (
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// AddressProvider is the interface types must implement to provide
// addresses to a Bootstrapper.
type AddressProvider interface {
	Lookup() ([]string, error)
}

// Bootstrapper performs a bootstrap of this node.
type Bootstrapper struct {
	provider     AddressProvider
	expect       int
	joinAttempts int
	joinInterval time.Duration
	tlsConfig    *tls.Config

	logger   *log.Logger
	Interval time.Duration
}

// NewBootstrapper returns an instance of a Bootstrapper.
func NewBootstrapper(p AddressProvider, expect int, joinAttempts int, joinInterval time.Duration,
	tlsConfig *tls.Config) *Bootstrapper {
	bs := &Bootstrapper{
		provider:     p,
		expect:       expect,
		joinAttempts: joinAttempts,
		joinInterval: joinInterval,
		tlsConfig:    &tls.Config{InsecureSkipVerify: true},
		logger:       log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
		Interval:     jitter(5 * time.Second),
	}
	if tlsConfig != nil {
		bs.tlsConfig = tlsConfig
	}
	return bs
}

// Boot performs the bootstrapping process for this node. This means it will
// ensure this node becomes part of a cluster. It does this by either joining
// an exisiting cluster by explicitly joining it through one of these nodes,
// or by notifying those nodes that it exists, allowing a cluster-wide bootstap
// take place. It will keep trying the boot operation until an explicit join
// succeeds, the given channel is closed, an error occurs that it can't handle,
// or the timeout expires.
//
// id and raftAddr are those of the node calling Boot. All operations performed
// by this function are done as a voting node.
func (b *Bootstrapper) Boot(id, raftAddr string, ch chan struct{}, timeout time.Duration) error {

	timeoutT := time.NewTimer(timeout)
	defer timeoutT.Stop()
	tickerT := time.NewTimer(jitter(time.Millisecond))
	defer tickerT.Stop()

	for {
		select {
		case <-ch:
			b.logger.Println("boot operation canceled")
			return nil
		case <-timeoutT.C:
			return fmt.Errorf("boot operation timed out after %s", timeout)
		case <-tickerT.C:
			tickerT.Reset(jitter(b.Interval)) // Move to longer-period polling

			targets, err := b.provider.Lookup()
			if err != nil {
				return fmt.Errorf("provider loopup failed %s", err.Error())
			}
			if len(targets) < b.expect {
				continue
			}

			// Try an explicit join.
			if j, err := Join("", targets, id, raftAddr, true, b.joinAttempts,
				b.joinInterval, b.tlsConfig); err == nil {
				b.logger.Printf("succeeded directly joining cluster via node at %s", j)
				return nil
			}

			// Join didn't work, so perform a notify.
			if err := b.notify(targets, id, raftAddr); err != nil {
				b.logger.Printf("failed to notify %s, retrying", targets)
			}
		}
	}

	return nil
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

// jitter adds a little bit of randomness to a given duration. This is
// useful to prevent nodes across the cluster performing certain operations
// all at the same time.
func jitter(duration time.Duration) time.Duration {
	return duration + time.Duration(rand.Float64()*float64(duration))
}
