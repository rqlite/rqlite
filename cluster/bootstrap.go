package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	rurl "github.com/rqlite/rqlite/http/url"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var (
	// ErrBootTimeout is returned when a boot operation does not
	// complete within the timeout.
	ErrBootTimeout = errors.New("boot timeout")
)

// BootStatus is status of the boot process, after it has completed.
type BootStatus int

const (
	BootUnknown BootStatus = iota
	BootJoin
	BootDone
	BootTimeout
)

// String returns a string representation of the BootStatus.
func (b BootStatus) String() string {
	switch b {
	case BootUnknown:
		return "unknown"
	case BootJoin:
		return "join"
	case BootDone:
		return "done"
	case BootTimeout:
		return "timeout"
	default:
		panic("unknown boot status")
	}
}

// AddressProvider is the interface types must implement to provide
// addresses to a Bootstrapper.
type AddressProvider interface {
	Lookup() ([]string, error)
}

// Bootstrapper performs a bootstrap of this node.
type Bootstrapper struct {
	provider  AddressProvider
	tlsConfig *tls.Config

	joiner *Joiner

	username string
	password string

	logger   *log.Logger
	Interval time.Duration

	bootStatusMu sync.RWMutex
	bootStatus   BootStatus
}

// NewBootstrapper returns an instance of a Bootstrapper.
func NewBootstrapper(p AddressProvider, tlsConfig *tls.Config) *Bootstrapper {
	bs := &Bootstrapper{
		provider:  p,
		tlsConfig: tlsConfig,
		joiner:    NewJoiner("", 1, 0, tlsConfig),
		logger:    log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
		Interval:  2 * time.Second,
	}
	return bs
}

// SetBasicAuth sets Basic Auth credentials for any bootstrap attempt.
func (b *Bootstrapper) SetBasicAuth(username, password string) {
	b.username, b.password = username, password
}

// Boot performs the bootstrapping process for this node. This means it will
// ensure this node becomes part of a cluster. It does this by either joining
// an existing cluster by explicitly joining it through one of these nodes,
// or by notifying those nodes that it exists, allowing a cluster-wide bootstap
// take place.
//
// Returns nil if the boot operation was successful, or if done() ever returns
// true. done() is periodically polled by the boot process. Returns an error
// the boot process encounters an unrecoverable error, or booting does not
// occur within the given timeout.
//
// id and raftAddr are those of the node calling Boot.  All operations
// performed by this function are done as a voting node.
func (b *Bootstrapper) Boot(id, raftAddr string, done func() bool, timeout time.Duration) error {
	timeoutT := time.NewTimer(timeout)
	defer timeoutT.Stop()
	tickerT := time.NewTimer(jitter(time.Millisecond))
	defer tickerT.Stop()

	for {
		select {
		case <-timeoutT.C:
			b.setBootStatus(BootTimeout)
			return ErrBootTimeout

		case <-tickerT.C:
			if done() {
				b.logger.Printf("boot operation marked done")
				b.setBootStatus(BootDone)
				return nil
			}
			tickerT.Reset(jitter(b.Interval)) // Move to longer-period polling

			targets, err := b.provider.Lookup()
			if err != nil {
				b.logger.Printf("provider lookup failed %s", err.Error())
			}

			if len(targets) == 0 {
				continue
			}

			// Try an explicit join first. Joining an existing cluster is always given priority
			// over trying to form a new cluster.
			b.joiner.SetBasicAuth(b.username, b.password)
			if j, err := b.joiner.Do(targets, id, raftAddr, true); err == nil {
				b.logger.Printf("succeeded directly joining cluster via node at %s", j)
				b.setBootStatus(BootJoin)
				return nil
			}

			// This is where we have to be careful. This node failed to join with any node
			// in the targets list. This could be because none of the nodes are contactable,
			// or none of the nodes are in a functioning cluster with a leader. That means that
			// this node could be part of a set nodes that are bootstrapping to form a cluster
			// de novo. For that to happen it needs to now let the other nodes know it is here.
			// If this is a new cluster, some node will then reach the bootstrap-expect value,
			// form the cluster, beating all other nodes to it.
			if err := b.notify(targets, id, raftAddr); err != nil {
				b.logger.Printf("failed to notify all targets: %s (%s, will retry)", targets,
					err.Error())
			} else {
				b.logger.Printf("succeeded notifying all targets: %s", targets)
			}
		}
	}
}

// Status returns the reason for the boot process completing.
func (b *Bootstrapper) Status() BootStatus {
	b.bootStatusMu.RLock()
	defer b.bootStatusMu.RUnlock()
	return b.bootStatus
}

func (b *Bootstrapper) notify(targets []string, id, raftAddr string) error {
	// Create and configure the client to connect to the other node.
	tr := &http.Transport{
		TLSClientConfig:   b.tlsConfig,
		ForceAttemptHTTP2: true,
	}
	client := &http.Client{Transport: tr}

	buf, err := json.Marshal(map[string]interface{}{
		"id":   id,
		"addr": raftAddr,
	})
	if err != nil {
		return err
	}

	for _, t := range targets {
		// Check for protocol scheme, and insert default if necessary.
		fullTarget := rurl.NormalizeAddr(fmt.Sprintf("%s/notify", t))

	TargetLoop:
		for {
			req, err := http.NewRequest("POST", fullTarget, bytes.NewReader(buf))
			if err != nil {
				return err
			}
			if b.username != "" && b.password != "" {
				req.SetBasicAuth(b.username, b.password)
			}
			req.Header.Add("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("failed to post notification to node at %s: %s",
					rurl.RemoveBasicAuth(fullTarget), err)
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
				b.logger.Printf("succeeded notifying target: %s", rurl.RemoveBasicAuth(fullTarget))
				break TargetLoop
			case http.StatusBadRequest:
				// One possible cause is that the target server is listening for HTTPS, but
				// an HTTP attempt was made. Switch the protocol to HTTPS, and try again.
				// This can happen when using various disco approaches, since it doesn't
				// record information about which protocol a registered node is actually using.
				if strings.HasPrefix(fullTarget, "https://") {
					// It's already HTTPS, give up.
					return fmt.Errorf("failed to notify node at %s: %s", rurl.RemoveBasicAuth(fullTarget),
						resp.Status)
				}
				fullTarget = rurl.EnsureHTTPS(fullTarget)
			default:
				return fmt.Errorf("failed to notify node at %s: %s",
					rurl.RemoveBasicAuth(fullTarget), resp.Status)
			}

		}
	}
	return nil
}

func (b *Bootstrapper) setBootStatus(status BootStatus) {
	b.bootStatusMu.Lock()
	defer b.bootStatusMu.Unlock()
	b.bootStatus = status
}

type stringAddressProvider struct {
	ss []string
}

func (s *stringAddressProvider) Lookup() ([]string, error) {
	return s.ss, nil
}

// NewAddressProviderString wraps an AddressProvider around a string slice.
func NewAddressProviderString(ss []string) AddressProvider {
	return &stringAddressProvider{ss}
}

// jitter adds a little bit of randomness to a given duration. This is
// useful to prevent nodes across the cluster performing certain operations
// all at the same time.
func jitter(duration time.Duration) time.Duration {
	return duration + time.Duration(rand.Float64()*float64(duration))
}
