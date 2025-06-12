package cdc

import (
	"crypto/tls"
	"sync"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

const (
	leaderChanLen = 5 // Support any fast back-to-back leadership changes.
)

// Cluster is an interface that defines methods for cluster management.
type Cluster interface {
	// IsLeader returns true if the node is the leader of the cluster.
	IsLeader() bool

	// RegisterLeaderChange registers the given channel which will receive
	// a signal when the node detects that the Leader changes.
	RegisterLeaderChange(c chan<- struct{})
}

// Store is an interface that defines methods for executing commands and querying
// the state of the store. It is used by the CDC service to read and write its own state.
type Store interface {
	// Execute allows us to write state to the store.
	Execute(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, error)

	// Query allows us to read state from the store.
	Query(qr *command.QueryRequest) ([]*command.QueryRows, error)
}

// Service is a CDC service that reads events from a channel and processes them.
// It is used to stream changes to a HTTP endpoint.
type Service struct {
	clstr Cluster
	str   Store

	in       <-chan *command.CDCEvents
	tlsConfg *tls.Config

	endpoint      string
	maxBatchSz    int64
	maxBatchDelay int64

	wg   sync.WaitGroup
	done chan struct{}
}

// NewService creates a new CDC service.
func NewService(in <-chan *command.CDCEvents, endpoint string, tlsConfig *tls.Config, maxBatchSz, maxBatchDelay int64) *Service {
	return &Service{
		clstr:         nil,
		str:           nil,
		in:            in,
		endpoint:      endpoint,
		tlsConfg:      tlsConfig,
		maxBatchSz:    maxBatchSz,
		maxBatchDelay: maxBatchDelay,
		done:          make(chan struct{}),
	}
}

// Start starts the CDC service.
func (s *Service) Start() {
	s.wg.Add(1)
	go s.readEvents()

	obCh := make(chan struct{}, leaderChanLen)
	s.clstr.RegisterLeaderChange(obCh)
}

// Stop stops the CDC service.
func (s *Service) Stop() {
	close(s.done)
	s.wg.Wait()
}

func (s *Service) readEvents() {
	defer s.wg.Done()
	<-s.done
	return
}
