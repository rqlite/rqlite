package cdc

import (
	"crypto/tls"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
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
	Execute(er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error)

	// Query allows us to read state from the store.
	Query(qr *proto.QueryRequest) ([]*proto.QueryRows, error)
}

// Service is a CDC service that reads events from a channel and processes them.
// It is used to stream changes to a HTTP endpoint.
type Service struct {
	clstr Cluster
	str   Store

	in        <-chan *proto.CDCEvents
	tlsConfig *tls.Config

	endpoint      string
	maxBatchSz    int
	maxBatchDelay time.Duration

	wg   sync.WaitGroup
	done chan struct{}
}

// NewService creates a new CDC service.
func NewService(clstr Cluster, str Store, in <-chan *proto.CDCEvents, endpoint string, tlsConfig *tls.Config, maxBatchSz int, maxBatchDelay time.Duration) *Service {
	return &Service{
		clstr:         clstr,
		str:           str,
		in:            in,
		endpoint:      endpoint,
		tlsConfig:     tlsConfig,
		maxBatchSz:    maxBatchSz,
		maxBatchDelay: maxBatchDelay,
		done:          make(chan struct{}),
	}
}

// Start starts the CDC service.
func (s *Service) Start() error {
	if err := s.createStateTable(); err != nil {
		return err
	}
	s.wg.Add(1)
	go s.readEvents()

	obCh := make(chan struct{}, leaderChanLen)
	s.clstr.RegisterLeaderChange(obCh)
	return nil
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

func (s *Service) createStateTable() error {
	er := executeRequestFromString(`
CREATE TABLE IF NOT EXISTS_rqlite_cdc_state (
    k         TEXT PRIMARY KEY,
    v_blob    BLOB,
    v_text    TEXT,
    v_int     INTEGER
)`)
	_, err := s.str.Execute(er)
	return err

}

func executeRequestFromString(s string) *proto.ExecuteRequest {
	return executeRequestFromStrings([]string{s}, false, false)
}

// executeRequestFromStrings converts a slice of strings into a proto.ExecuteRequest
func executeRequestFromStrings(s []string, timings, tx bool) *proto.ExecuteRequest {
	stmts := make([]*proto.Statement, len(s))
	for i := range s {
		stmts[i] = &proto.Statement{
			Sql: s[i],
		}
	}
	return &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}
