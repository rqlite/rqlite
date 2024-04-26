package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/cluster/proto"
	command "github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/random"
)

var (
	// ErrBootTimeout is returned when a boot operation does not
	// complete within the timeout.
	ErrBootTimeout = errors.New("boot timeout")

	// ErrBootCanceled is returned when a boot operation is
	// canceled.
	ErrBootCanceled = errors.New("boot canceled")
)

// BootStatus is the reason the boot process completed.
type BootStatus int

const (
	// BootUnknown is the initial state of the boot process.
	BootUnknown BootStatus = iota

	// BootJoin means boot completed due to a successful join.
	BootJoin

	// BootDone means boot completed due to Done being "true".
	BootDone

	// BootTimeout means the boot process timed out.
	BootTimeout

	// BootCanceled means the boot process was canceled.
	BootCanceled
)

// Suffrage is the type of suffrage -- voting or non-voting -- a node has.
type Suffrage int

const (
	SuffrageUnknown Suffrage = iota
	Voter
	NonVoter
)

// VoterSuffrage returns a Suffrage based on the given boolean.
func VoterSuffrage(b bool) Suffrage {
	if b {
		return Voter
	}
	return NonVoter
}

// String returns a string representation of the Suffrage.
func (s Suffrage) String() string {
	switch s {
	case Voter:
		return "voter"
	case NonVoter:
		return "non-voter"
	default:
		panic("unknown suffrage")
	}
}

// IsVoter returns whether the Suffrage is a Voter.
func (s Suffrage) IsVoter() bool {
	return s == Voter
}

// IsNonVoter returns whether the Suffrage is a NonVoter.
func (s Suffrage) IsNonVoter() bool {
	return s == NonVoter
}

const (
	requestTimeout  = 5 * time.Second
	numJoinAttempts = 1
	bootInterval    = 2 * time.Second
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
	provider AddressProvider

	client *Client
	creds  *proto.Credentials

	logger   *log.Logger
	Interval time.Duration

	bootStatusMu sync.RWMutex
	bootStatus   BootStatus
}

// NewBootstrapper returns an instance of a Bootstrapper.
func NewBootstrapper(p AddressProvider, client *Client) *Bootstrapper {
	bs := &Bootstrapper{
		provider: p,
		client:   client,
		logger:   log.New(os.Stderr, "[cluster-bootstrap] ", log.LstdFlags),
		Interval: bootInterval,
	}
	return bs
}

// SetCredentials sets the credentials for the Bootstrapper.
func (b *Bootstrapper) SetCredentials(creds *proto.Credentials) {
	b.creds = creds
}

// Boot performs the bootstrapping process for this node. This means it will
// ensure this node becomes part of a cluster. It does this by either:
//   - joining an existing cluster by explicitly joining it through a node returned
//     by the AddressProvider, or
//   - if it's a Voting node, notifying all nodes returned by the AddressProvider
//     that it exists, potentially allowing a cluster-wide bootstrap take place
//     which will include this node.
//
// Returns nil if the boot operation was successful, or if done() ever returns
// true. done() is periodically polled by the boot process. Returns an error
// the boot process encounters an unrecoverable error, or booting does not
// occur within the given timeout.
//
// id and raftAddr are those of the node calling Boot. suf is whether this node
// is a Voter or NonVoter.
func (b *Bootstrapper) Boot(ctx context.Context, id, raftAddr string, suf Suffrage, done func() bool, timeout time.Duration) error {
	timeoutT := time.NewTimer(timeout)
	defer timeoutT.Stop()
	tickerT := time.NewTimer(random.Jitter(time.Millisecond))
	defer tickerT.Stop()

	joiner := NewJoiner(b.client, numJoinAttempts, requestTimeout)
	joiner.SetCredentials(b.creds)
	for {
		select {
		case <-ctx.Done():
			b.setBootStatus(BootCanceled)
			return ErrBootCanceled

		case <-timeoutT.C:
			b.setBootStatus(BootTimeout)
			return ErrBootTimeout

		case <-tickerT.C:
			if done() {
				b.logger.Printf("boot operation marked done")
				b.setBootStatus(BootDone)
				return nil
			}
			tickerT.Reset(random.Jitter(b.Interval)) // Move to longer-period polling

			targets, err := b.provider.Lookup()
			if err != nil {
				b.logger.Printf("provider lookup failed %s", err.Error())
			}
			if len(targets) == 0 {
				continue
			}

			// Try an explicit join first. Joining an existing cluster is always given priority
			// over trying to form a new cluster.
			if j, err := joiner.Do(ctx, targets, id, raftAddr, suf); err == nil {
				b.logger.Printf("succeeded directly joining cluster via node at %s as %s", j, suf)
				b.setBootStatus(BootJoin)
				return nil
			}

			if suf.IsVoter() {
				// This is where we have to be careful. This node failed to join with any node
				// in the targets list. This could be because none of the nodes are contactable,
				// or none of the nodes are in a functioning cluster with a leader. That means that
				// this node could be part of a set nodes that are bootstrapping to form a cluster
				// de novo. For that to happen it needs to now let the other nodes know it is here.
				// If this is a new cluster, some node will then reach the bootstrap-expect value
				// first, form the cluster, beating all other nodes to it.
				if err := b.notify(targets, id, raftAddr); err != nil {
					b.logger.Printf("failed to notify all targets: %s (%s, will retry)", targets,
						err.Error())
				} else {
					b.logger.Printf("succeeded notifying all targets: %s", targets)
				}
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
	nr := &command.NotifyRequest{
		Address: raftAddr,
		Id:      id,
	}
	for _, t := range targets {
		if err := b.client.Notify(nr, t, b.creds, requestTimeout); err != nil {
			return fmt.Errorf("failed to notify node at %s: %s", t, err)
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
