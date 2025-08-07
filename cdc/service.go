package cdc

import (
	"bytes"
	"crypto/tls"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/internal/rsync"
	"github.com/rqlite/rqlite/v8/queue"
)

const (
	cdcDB            = "cdc.db"
	highWatermarkKey = "high_watermark"
	leaderChanLen    = 5 // Support any fast back-to-back leadership changes.
)

const (
	numDroppedNotLeader    = "dropped_not_leader"
	numDroppedFailedToSend = "dropped_failed_to_send"
	numRetries             = "retries"
	numSent                = "sent_events"
)

// stats captures stats for the CDC Service.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("cdc-service")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numDroppedNotLeader, 0)
	stats.Add(numDroppedFailedToSend, 0)
	stats.Add(numRetries, 0)
	stats.Add(numSent, 0)
}

// Cluster is an interface that defines methods for cluster management.
type Cluster interface {
	// IsLeader returns true if the node is the leader of the cluster.
	IsLeader() bool

	// RegisterLeaderChange registers the given channel which will receive
	// a signal when the node detects that the Leader changes.
	RegisterLeaderChange(c chan<- bool)
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
	dir   string
	clstr Cluster
	str   Store

	// in is the channel from which the CDC events are read. This channel is expected
	in <-chan *proto.CDCEvents

	// logOnly indicates whether the CDC service should only log events and not
	// send them to the configured endpoint. This is mostly useful for testing.
	logOnly bool

	// endpoint is the HTTP endpoint to which the CDC events are sent.
	endpoint string

	// httpClient is the HTTP client used to send requests to the endpoint.
	httpClient *http.Client

	// tlsConfig is the TLS configuration used for the HTTP client.
	tlsConfig *tls.Config

	// transmitTimeout is the timeout for transmitting events to the endpoint.
	transmitTimeout time.Duration

	// transmitMaxRetries is the maximum number of retries for sending events to the endpoint.
	transmitMaxRetries int

	// TransmitMinBackoff is the delay between retries for sending events to the endpoint.
	transmitMinBackoff time.Duration

	// TransmitMaxBackoff is the maximum backoff time for retries when using exponential backoff.
	transmitMaxBackoff time.Duration

	// transmitRetryPolicy defines the retry policy to use when sending events to the endpoint.
	transmitRetryPolicy RetryPolicy

	// maxBatchSz is the maximum number of events to send in a single batch to the endpoint.
	maxBatchSz int

	// maxBatchDelay is the maximum delay before sending a batch of events, regardless
	// of the number of events ready for sending. This is used to ensure that
	// we don't wait too long for a batch to fill up.
	maxBatchDelay time.Duration

	// fifo is the persistent queue that collects CDC events generated on this node.
	// The CDC service stores these events regardless of its leader status. This allows
	// the service to recover from leader changes and ensure that every event is transmitted
	// at least once to the webhook endpoint.
	fifo *Queue

	// queue implements the batching of CDC events before transmission to the webhook. The
	// contents of this queue do not persist across restarts or leader changes.
	batcher *queue.Queue[*proto.CDCEvents]

	// highWatermark is the index of the last event that was successfully sent to the webhook
	// by the cluster (which is not necessarily the same thing as this node).
	highWatermark atomic.Uint64

	// highWatermarkInterval is the interval at which the high watermark is written to the store.
	// This is used to ensure that the high watermark is written periodically,
	highWatermarkInterval time.Duration

	// highWatermarkingDisabled indicates whether high watermarking is disabled.
	// If true, the service will not write or read the high watermark from the store.
	highWatermarkingDisabled rsync.AtomicBool

	// Channel to receive notifications of leader changes.
	leaderObCh chan bool

	// Channel to receive high watermark updates from the cluster.
	hwmObCh chan uint64

	// For CDC shutdown.
	wg   sync.WaitGroup
	done chan struct{}

	logger *log.Logger
}

// NewService creates a new CDC service.
func NewService(dir string, clstr Cluster, str Store, in <-chan *proto.CDCEvents, cfg *Config) (*Service, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: cfg.TLSConfig,
		},
		Timeout: cfg.TransmitTimeout,
	}

	srv := &Service{
		dir:                   dir,
		clstr:                 clstr,
		str:                   str,
		in:                    in,
		logOnly:               cfg.LogOnly,
		endpoint:              cfg.Endpoint,
		httpClient:            httpClient,
		tlsConfig:             cfg.TLSConfig,
		transmitTimeout:       cfg.TransmitTimeout,
		transmitMaxRetries:    cfg.TransmitMaxRetries,
		transmitMinBackoff:    cfg.TransmitMinBackoff,
		transmitMaxBackoff:    cfg.TransmitMaxBackoff,
		transmitRetryPolicy:   cfg.TransmitRetryPolicy,
		maxBatchSz:            cfg.MaxBatchSz,
		maxBatchDelay:         cfg.MaxBatchDelay,
		highWatermarkInterval: cfg.HighWatermarkInterval,
		batcher:               queue.New[*proto.CDCEvents](cfg.MaxBatchSz, cfg.MaxBatchSz, cfg.MaxBatchDelay),
		leaderObCh:            make(chan bool, leaderChanLen),
		hwmObCh:               make(chan uint64, leaderChanLen),
		done:                  make(chan struct{}),
		logger:                log.New(os.Stdout, "[cdc-service] ", log.LstdFlags),
	}

	fifo, err := NewQueue(filepath.Join(dir, cdcDB))
	if err != nil {
		return nil, err
	}
	srv.fifo = fifo

	srv.highWatermark.Store(0)
	srv.highWatermarkingDisabled.SetBool(cfg.HighWatermarkingDisabled)
	return srv, nil
}

// Start starts the CDC service.
func (s *Service) Start() error {
	if s.highWatermarkingDisabled.IsNot() {
		if err := s.createStateTable(); err != nil {
			return err
		}
	}
	s.wg.Add(2)
	go s.writeToFIFO()
	go s.mainLoop()

	s.clstr.RegisterLeaderChange(s.leaderObCh)
	s.logger.Println("service started")
	return nil
}

// SetHighWatermarking enables or disables high watermarking.
func (s *Service) SetHighWatermarking(enabled bool) {
	s.highWatermarkingDisabled.SetBool(!enabled)
}

// Stop stops the CDC service.
func (s *Service) Stop() {
	if s.clstr.IsLeader() && s.highWatermarkingDisabled.IsNot() {
		// Best effort to write the high watermark before stopping.
		s.writeHighWatermark(s.highWatermark.Load())
	}
	close(s.done)
	s.fifo.Close()
	s.wg.Wait()
}

// HighWatermark returns the high watermark of the CDC service. This
// is the index of the last event that was successfully sent to the webhook.
func (s *Service) HighWatermark() uint64 {
	return s.highWatermark.Load()
}

func (s *Service) writeToFIFO() {
	defer s.wg.Done()
	for {
		select {
		case o := <-s.in:
			b, err := command.MarshalCDCEvents(o)
			if err != nil {
				s.logger.Printf("error marshalling CDC events: %v", err)
				continue
			}
			if err := s.fifo.Enqueue(o.Index, b); err != nil {
				s.logger.Printf("error enqueueing CDC events: %v", err)
			}
		case <-s.done:
			return
		}
	}
}

func (s *Service) readFromFIFO() (chan struct{}, chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})
	ch := s.fifo.Events()
	go func() {
		for {
			select {
			case <-stop:
				close(done)
				return
			case data := <-ch:
				events, err := command.UnmarshalCDCEvents(data.Data)
				if err != nil {
					s.logger.Printf("error unmarshalling CDC events from FIFO: %v", err)
					continue
				}
				if len(events.Events) == 0 {
					panic("xxxx")
					s.logger.Println("received empty CDC events from FIFO, skipping")
				}
				s.batcher.Write([]*proto.CDCEvents{events}, nil)
			}
		}
	}()
	return stop, done
}

func (s *Service) mainLoop() {
	defer s.wg.Done()

	// This ticker is used to periodically broadcast the high watermark to the cluster.
	hwmTicker := time.NewTicker(s.highWatermarkInterval)
	defer hwmTicker.Stop()

	var stop chan struct{}
	var done chan struct{}

	preHWM := s.highWatermark.Load()
	for {
		select {
		case <-hwmTicker.C:
			if s.highWatermarkingDisabled.Is() {
				continue
			}
			if s.highWatermark.Load() == preHWM {
				// Nothing to do.
				continue
			}
			preHWM = s.highWatermark.Load()
			if s.clstr.IsLeader() {
				if err := s.writeHighWatermark(s.highWatermark.Load()); err != nil {
					s.logger.Printf("error writing high watermark to store: %v", err)
				}
			}

		case hwm := <-s.hwmObCh:
			s.logger.Println("received high watermark update:", hwm)
			if hwm > s.highWatermark.Load() {
				s.highWatermark.Store(hwm)
				// This means all events up to this high watermark have been
				// successfully sent to the webhook by the cluster. We can
				// delete all events up and including that point from our FIFO.
			}

		case isLeader := <-s.leaderObCh:
			s.logger.Println("is leader:", isLeader)
			// If not leader then reset batching queue and get ready to start
			// retransmitting events from high-water mark. If we have become
			// the leader then start reading from the batching queue. XXX TBD
			if isLeader {
				stop, done = s.readFromFIFO()
			} else {
				if stop == nil {
					continue
				}
				close(stop)
				stop = nil
				<-done
				done = nil
			}

		case batch := <-s.batcher.C:
			if batch == nil || len(batch.Objects) == 0 {
				continue
			}

			// Only the Leader actually sends events.
			if !s.clstr.IsLeader() {
				stats.Add(numDroppedNotLeader, int64(len(batch.Objects)))
				continue
			}

			b, err := MarshalToEnvelopeJSON(batch.Objects)
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

			nAttempts := 0
			retryDelay := s.transmitMinBackoff
			sentOK := false
			for {
				nAttempts++
				if s.logOnly {
					s.logger.Println(string(b))
					sentOK = true
					break
				}

				resp, err := s.httpClient.Do(req)
				if err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted) {
					resp.Body.Close()
					sentOK = true
					break
				}
				if nAttempts == s.transmitMaxRetries {
					s.logger.Printf("failed to send batch to endpoint after %d retries, last error: %v", nAttempts, err)
					stats.Add(numDroppedFailedToSend, int64(len(batch.Objects)))
					break
				}

				if s.transmitRetryPolicy == ExponentialRetryPolicy {
					retryDelay *= 2
					if retryDelay > s.transmitMaxBackoff {
						retryDelay = s.transmitMaxBackoff
					}
				}
				stats.Add(numRetries, 1)
				time.Sleep(retryDelay)
			}
			if sentOK {
				s.highWatermark.Store(batch.Objects[len(batch.Objects)-1].Index)
				stats.Add(numSent, int64(len(batch.Objects)))
			}

		case <-s.done:
			return
		}
	}
}

func (s *Service) createStateTable() error {
	er := executeRequestFromString(`
CREATE TABLE IF NOT EXISTS _rqlite_cdc_state (
    k         TEXT PRIMARY KEY,
    v_blob    BLOB,
    v_text    TEXT,
    v_int     INTEGER
)`)
	_, err := s.str.Execute(er)
	return err
}

func (s *Service) writeHighWatermark(value uint64) error {
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
