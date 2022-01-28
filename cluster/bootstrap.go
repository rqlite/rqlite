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
	"time"

	httpd "github.com/rqlite/rqlite/http"
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
	provider  AddressProvider
	expect    int
	tlsConfig *tls.Config

	logger   *log.Logger
	Interval time.Duration
}

// NewBootstrapper returns an instance of a Bootstrapper.
func NewBootstrapper(p AddressProvider, expect int, tlsConfig *tls.Config) *Bootstrapper {
	bs := &Bootstrapper{
		provider:  p,
		expect:    expect,
		tlsConfig: &tls.Config{InsecureSkipVerify: true},
		logger:    log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
		Interval:  jitter(5 * time.Second),
	}
	if tlsConfig != nil {
		bs.tlsConfig = tlsConfig
	}
	return bs
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

	notifySuccess := false
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
				b.logger.Printf("provider loopup failed %s", err.Error())
			}
			if len(targets) < b.expect {
				continue
			}

			// Try an explicit join.
			if j, err := Join("", targets, id, raftAddr, true, 1, 0, b.tlsConfig); err == nil {
				b.logger.Printf("succeeded directly joining cluster via node at %s", j)
				return nil
			}

			// Join didn't work, so perhaps perform a notify if we haven't done
			// one yet.
			if !notifySuccess {
				if err := b.notify(targets, id, raftAddr); err != nil {
					b.logger.Printf("failed to notify %s, retrying", targets)
				} else {
					b.logger.Printf("succeeded notifying %s", targets)
					notifySuccess = true
				}
			}
		}
	}
}

func (b *Bootstrapper) notify(targets []string, id, raftAddr string) error {
	// Create and configure the client to connect to the other node.
	tr := &http.Transport{
		TLSClientConfig: b.tlsConfig,
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
		fullTarget := httpd.NormalizeAddr(fmt.Sprintf("%s/notify", t))

	NodeLoop:
		for {
			resp, err := client.Post(fullTarget, "application/json", bytes.NewReader(buf))
			if err != nil {
				return err
				// time.Sleep(bs.joinInterval) // need to count loops....? Or this just does one loop?
				// continue
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusOK:
				break NodeLoop
			case http.StatusBadRequest:
				// One possible cause is that the target server is listening for HTTPS, but
				// an HTTP attempt was made. Switch the protocol to HTTPS, and try again.
				// This can happen when using various disco approaches, since it doesn't
				// record information about which protocol a registered node is actually using.
				if strings.HasPrefix(fullTarget, "https://") {
					// It's already HTTPS, give up.
					return fmt.Errorf("failed to notify node: %s", resp.Status)
				}
				fullTarget = httpd.EnsureHTTPS(fullTarget)
			default:
				return fmt.Errorf("failed to notify node: %s", resp.Status)
			}

		}
	}
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
