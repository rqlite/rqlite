package cdc

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/queue"
)

const (
	highWatermarkKey = "high_watermark"
	leaderChanLen    = 5 // Support any fast back-to-back leadership changes.
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

	// endpoint is the HTTP endpoint to which the CDC events are sent.
	endpoint string

	// httpClient is the HTTP client used to send requests to the endpoint.
	httpClient *http.Client

	// maxBatchSz is the maximum number of events to send in a single batch to the endpoint.
	maxBatchSz int

	// maxBatchDelay is the maximum delay before sending a batch of events, regardless
	// of the number of events ready for sending. This is used to ensure that
	// we don't wait too long for a batch to fill up.
	maxBatchDelay time.Duration

	// queue is a queue of events to be sent to the webhook. It implements the
	// batching and timeout logic.
	queue *queue.Queue[*proto.CDCEvents]

	// highWatermark is the index of the last event that was successfully sent to the webhook
	// by the cluster (which is not necessarily the same thing as this node).
	highWatermark atomic.Uint64

	// For CDC shutdown.
	wg   sync.WaitGroup
	done chan struct{}

	logger *log.Logger
}

// NewService creates a new CDC service.
func NewService(clstr Cluster, str Store, in <-chan *proto.CDCEvents, endpoint string, tlsConfig *tls.Config, maxBatchSz int, maxBatchDelay time.Duration) *Service {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: 5 * time.Second,
	}

	return &Service{
		clstr:         clstr,
		str:           str,
		in:            in,
		tlsConfig:     tlsConfig,
		endpoint:      endpoint,
		httpClient:    httpClient,
		maxBatchSz:    maxBatchSz,
		maxBatchDelay: maxBatchDelay,
		queue:         queue.New[*proto.CDCEvents](maxBatchSz, maxBatchSz, maxBatchDelay),
		done:          make(chan struct{}),
		logger:        log.New(os.Stdout, "[cdc service] ", log.LstdFlags),
	}
}

// Start starts the CDC service.
func (s *Service) Start() error {
	if err := s.createStateTable(); err != nil {
		return err
	}
	s.wg.Add(2)
	go s.readEvents()
	go s.postEvents()

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
	for {
		select {
		case o := <-s.in:
			// Right now just write the event to the queue. Events should be
			// persisted to a disk-based queue for replay on Leader change.
			s.queue.Write([]*proto.CDCEvents{o}, nil)
		case <-s.done:
			return
		}
	}
}

func (s *Service) postEvents() {
	defer s.wg.Done()
	for {
		select {
		case batch := <-s.queue.C:
			if batch == nil || len(batch.Objects) == 0 {
				continue
			}

			// Only the Leader actually sends events.
			if !s.clstr.IsLeader() {
				continue
			}

			b, err := json.Marshal(batch.Objects)
			if err != nil {
				s.logger.Printf("error marshalling batch: %v", err)
				continue
			}

			req, err := http.NewRequest("POST", s.endpoint, bytes.NewReader(b))
			if err != nil {
				s.logger.Printf("error creating HTTP request for endpoint: %v", err)
				continue
			}
			req.Header.Set("Content-Type", "application/json")

			maxRetries := 5
			nAttempts := 0
			retryDelay := 500 * time.Millisecond
			for {
				nAttempts++
				resp, err := s.httpClient.Do(req)
				if err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted) {
					resp.Body.Close()
					s.writeHighWatermark(batch.Objects[len(batch.Objects)-1].K)
					break
				}
				if nAttempts >= maxRetries {
					s.logger.Printf("failed to send batch to endpoint after %d retries, last error: %v", nAttempts, err)
					break
				}
				retryDelay *= 2
				time.Sleep(retryDelay)
			}
		case <-s.done:
			return
		}
	}
}

func (s *Service) createStateTable() error {
	return nil
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

func (s *Service) writeHighWatermark(value uint64) error {
	return nil
	sql := fmt.Sprintf(`INSERT OR REPLACE INTO _rqlite_cdc_state(k, v_int) VALUES ('%s', %d)`, highWatermarkKey, value)
	er := executeRequestFromString(sql)
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
