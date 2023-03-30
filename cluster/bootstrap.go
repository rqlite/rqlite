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
	"net/url"
	"os"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var (
	// ErrBootTimeout is returned when a boot operation does not
	// complete within the timeout.
	ErrBootTimeout = errors.New("boot timeout")
)

// AddressProvider is the interface types must implement to provide
// addresses to a Bootstrapper.
type AddressProvider interface {
	Lookup() ([]string, error)
}

// Bootstrapper performs a bootstrap of this node.
type Bootstrapper struct {
	provider   AddressProvider
	httpClient *http.Client

	joiner *Joiner

	username string
	password string

	logger   *log.Logger
	Interval time.Duration
}

// NewBootstrapper returns an instance of a Bootstrapper.
func NewBootstrapper(p AddressProvider, httpTLSConfig *tls.Config) *Bootstrapper {
	bs := &Bootstrapper{
		provider: p,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   httpTLSConfig,
				ForceAttemptHTTP2: true,
			},
			Timeout: 10 * time.Second,
		},
		joiner:   NewJoiner("", 1, 0, httpTLSConfig),
		logger:   log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
		Interval: 2 * time.Second,
	}
	return bs
}

// SetBasicAuth sets Basic Auth credentials for any bootstrap attempt.
func (b *Bootstrapper) SetBasicAuth(username, password string) {
	b.username, b.password = username, password
	b.joiner.SetBasicAuth(b.username, b.password)
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
			return ErrBootTimeout

		case <-tickerT.C:
			if done() {
				b.logger.Printf("boot operation marked done")
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
			if j, err := b.joiner.Do(targets, id, raftAddr, true); err == nil {
				b.logger.Printf("succeeded directly joining cluster via node at %s", j)
				return nil
			}

			// This is where we have to be careful. This node failed to join with any node
			// in the targets list. This could be because none of the nodes are contactable,
			// or none of the nodes are in a functioning cluster with a leader. That means that
			// this node could be part of a set nodes that are bootstrapping to form a cluster
			// de novo. For that to happen it needs to now let the other nodes know it is here.
			// If this is a new cluster, some node will then reach the bootstrap-expect value,
			// form the cluster, beating all other nodes to it.
			urls, err := stringsToURLs(targets)
			if err != nil {
				b.logger.Printf("failed to convert targets to URLs: %s", err.Error())
				continue
			}
			if err := b.notify(urls, id, raftAddr); err != nil {
				b.logger.Printf("failed to notify all targets: %s (%s, will retry)", targets,
					err.Error())
			} else {
				b.logger.Printf("succeeded notifying all targets: %s", targets)
			}
		}
	}
}

func (b *Bootstrapper) notify(targets []*url.URL, id, raftAddr string) error {
	// Need to think about error handling. One or both? Which results in
	// an error back to the caller?
	for _, t := range targets {
		if strings.ToLower(t.Scheme) == "raft" {
			b.notifyRaft(t, id, raftAddr)
		} else {
			b.notifyHTTP(t, id, raftAddr)
		}
	}
	return nil
}

// notifyRaft is used to notify a remote node of this node's presence by contacting
// the remote node via its Raft address.
func (b *Bootstrapper) notifyRaft(u *url.URL, id, raftAddr string) error {
	// Pull the host out of the URL and use it to connect to the remote node.
	return nil
}

// notifyHTTP is used to notify a remote node of this node's presence. It
// does this by making an HTTP POST request to the /notify endpoint on the
// target node. If no scheme is specified in the URL, it will first try to
// use HTTP, and if that fails, it will try HTTPS. If HTTPS fails, the
// error is returned.
func (b *Bootstrapper) notifyHTTP(u *url.URL, id, raftAddr string) error {
	nURL := &url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   "/notify",
	}
	if nURL.Scheme == "" {
		nURL.Scheme = "http"
	}

	buf, err := json.Marshal(map[string]interface{}{
		"id":   id,
		"addr": raftAddr,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", nURL.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	if b.username != "" && b.password != "" {
		req.SetBasicAuth(b.username, b.password)
	}
	req.Header.Add("Content-Type", "application/json")
	for {
		resp, err := b.httpClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			b.logger.Printf("succeeded notifying target: %s", nURL.Redacted())
			return nil
		case http.StatusBadRequest:
			// One possible cause is that the target server is listening for HTTPS, but
			// an HTTP attempt was made. Switch the protocol to HTTPS, and try again.
			// This can happen when using various disco systems, since disco doesn't always
			// record information about which protocol a registered node is actually using.
			if nURL.Scheme == "https" {
				// It's already HTTPS, give up.
				return fmt.Errorf("failed to notify node at %s even after with HTTPS: %s",
					nURL.Redacted(), resp.Status)
			}
			nURL.Scheme = "https"
		default:
			return fmt.Errorf("failed to notify node at %s: %s",
				nURL.Redacted(), resp.Status)
		}
	}
}

func stringsToURLs(ss []string) ([]*url.URL, error) {
	urls := make([]*url.URL, 0, len(ss))
	for _, s := range ss {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		urls = append(urls, u)
	}
	return urls, nil
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
