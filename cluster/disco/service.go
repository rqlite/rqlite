package disco

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/internal/random"
	"github.com/rqlite/rqlite/v8/rqlog"
)

const (
	leaderChanLen = 5 // Support any fast back-to-back leadership changes.
)

// Client is the interface discovery clients must implement.
type Client interface {
	// GetLeader returns the current Leader stored in the KV store. If the Leader
	// is set, the returned ok flag will be true. If the Leader is not set, the
	// returned ok flag will be false.
	GetLeader() (id string, apiAddr string, addr string, ok bool, e error)

	// InitializeLeader sets the leader to the given details, but only if no leader
	// has already been set. This operation is a check-and-set type operation. If
	// initialization succeeds, ok is set to true, otherwise false.
	InitializeLeader(id, apiAddr, addr string) (bool, error)

	// SetLeader unconditionally sets the leader to the given details.
	SetLeader(id, apiAddr, addr string) error

	fmt.Stringer
}

// Store is the interface the consensus system must implement.
type Store interface {
	// IsLeader returns whether this node is the Leader.
	IsLeader() bool

	// RegisterLeaderChange registers a channel that will be notified when
	// a leadership change occurs.
	RegisterLeaderChange(c chan<- bool)
}

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

// IsVoter returns whether the Suffrage indicates a Voter.
func (s Suffrage) IsVoter() bool {
	return s == Voter
}

// Service represents a Discovery Service instance.
type Service struct {
	RegisterInterval time.Duration
	ReportInterval   time.Duration

	c   Client
	s   Store
	suf Suffrage

	logger rqlog.Logger

	mu          sync.Mutex
	lastContact time.Time
}

// NewService returns an instantiated Discovery Service.
func NewService(c Client, s Store, suf Suffrage) *Service {
	return &Service{
		c:                c,
		s:                s,
		suf:              suf,
		RegisterInterval: 3 * time.Second,
		ReportInterval:   10 * time.Second,
		logger:           rqlog.Default().WithName("[disco] ").WithOutput(os.Stderr),
	}
}

// Register registers this node with the discovery service. It will block
// until a) if the node is a voter, it registers itself, b) learns of another
// node it can use to join the cluster, or c) an unrecoverable error occurs.
func (s *Service) Register(id, apiAddr, addr string) (bool, string, error) {
	for {
		_, _, cRaftAddr, ok, err := s.c.GetLeader()
		if err != nil {
			s.logger.Printf("failed to get leader: %s", err.Error())
		}
		if ok {
			return false, cRaftAddr, nil
		}

		if s.suf.IsVoter() {
			ok, err = s.c.InitializeLeader(id, apiAddr, addr)
			if err != nil {
				s.logger.Printf("failed to initialize as Leader: %s", err.Error())
			}
			if ok {
				s.updateContact(time.Now())
				return true, addr, nil
			}
		}

		time.Sleep(random.Jitter(s.RegisterInterval))
	}
}

// StartReporting reports the details of this node to the discovery service,
// if, and only if, this node is the leader. The service will report
// anytime a leadership change is detected. It also does it periodically
// to deal with any intermittent issues that caused Leadership information
// to go stale.
func (s *Service) StartReporting(id, apiAddr, addr string) chan struct{} {
	ticker := time.NewTicker(s.ReportInterval)
	obCh := make(chan bool, leaderChanLen)
	s.s.RegisterLeaderChange(obCh)

	done := make(chan struct{})
	reportCh := make(chan struct{}, leaderChanLen)

	reportNonBlocking := func() {
		select {
		case reportCh <- struct{}{}:
		default:
			// If the channel is full, we don't block, and just drop the report.
			// This is fine, as the ticker will report periodically anyway.
			// This is useful if the discovery service is slow to respond, for example.
		}
	}

	go func() {
		for {
			select {
			case <-reportCh:
				if err := s.c.SetLeader(id, apiAddr, addr); err != nil {
					s.logger.Printf("failed to update discovery service with Leader details: %s",
						err.Error())
					continue
				}
				s.updateContact(time.Now())
			case <-done:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case isLeader := <-obCh:
				if isLeader {
					reportNonBlocking()
					s.logger.Printf("updated Leader API address to %s due to leadership change", apiAddr)
				}
			case <-ticker.C:
				// Report it periodically in case a previous update failed due, for example,
				// to a network issue contacting the discovery service. Do this by checking
				// the cluster system directly. This means we don't need to worry about
				// missing any leadership changes that occurred before this service
				// started.
				if s.s.IsLeader() {
					reportNonBlocking()
				}
			case <-done:
				return
			}
		}
	}()
	return done
}

// Stats returns diagnostic information on the disco service.
func (s *Service) Stats() (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]any{
		"mode":              s.c.String(),
		"register_interval": s.RegisterInterval,
		"report_interval":   s.ReportInterval,
		"last_contact":      s.lastContact,
	}, nil
}

func (s *Service) updateContact(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastContact = t
}
