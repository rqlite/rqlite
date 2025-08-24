package cdc

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	cdcjson "github.com/rqlite/rqlite/v8/cdc/json"
	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/proto"
	httpurl "github.com/rqlite/rqlite/v8/http/url"
	"github.com/rqlite/rqlite/v8/internal/rsync"
	"github.com/rqlite/rqlite/v8/queue"
)

const (
	cdcDB         = "fifo.db"
	hwmFile       = "hwm.json"
	leaderChanLen = 5 // Support any fast back-to-back leadership changes.
)

const (
	numDroppedNotLeader    = "dropped_not_leader"
	numDroppedFailedToSend = "dropped_failed_to_send"
	numRetries             = "retries"
	numSent                = "sent_events"
)

// hwmData represents the structure of the high watermark file.
type hwmData struct {
	HWM       uint64 `json:"hwm"`
	Timestamp string `json:"timestamp"`
}

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

// Cluster is an interface that defines methods for cluster management and communication.
type Cluster interface {
	// RegisterLeaderChange registers the given channel which will receive
	// a signal when the node detects that the Leader changes.
	RegisterLeaderChange(c chan<- bool)

	// RegisterHWMUpdate registers a channel to receive highwater mark updates.
	RegisterHWMUpdate(c chan<- uint64)

	// BroadcastHighWatermark sets the high watermark across the cluster.
	BroadcastHighWatermark(value uint64) error
}

// Service is a CDC service that reads events from a channel and processes them.
// It is used to stream changes to a HTTP endpoint.
type Service struct {
	serviceID   string
	nodeID      string
	dir         string
	hwmFilePath string
	clstr       Cluster

	// in is the channel from which the CDC events are read.
	in <-chan *proto.CDCIndexedEventGroup

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
	batcher *queue.Queue[*proto.CDCIndexedEventGroup]

	// highWatermark is the index of the last event that was successfully sent to the webhook
	// by the cluster (which is not necessarily the same thing as this node).
	highWatermark atomic.Uint64

	// highWatermarkInterval is the interval at which the high watermark is written to the store.
	// This is used to ensure that the high watermark is written periodically,
	highWatermarkInterval time.Duration

	// Channel to receive notifications of leader changes and store latest state.
	leaderObCh chan bool
	isLeader   rsync.AtomicBool

	// Channel to receive high watermark updates from the cluster.
	hwmObCh chan uint64

	// For CDC shutdown.
	wg   sync.WaitGroup
	done chan struct{}

	// For white box testing
	hwmLeaderUpdated   atomic.Uint64
	hwmFollowerUpdated atomic.Uint64

	logger *log.Logger
}

// NewService creates a new CDC service.
func NewService(nodeID, dir string, clstr Cluster, in <-chan *proto.CDCIndexedEventGroup, cfg *Config) (*Service, error) {
	// Build the TLS configuration from the config fields
	tlsConfig, err := cfg.TLSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: cfg.TransmitTimeout,
	}

	srv := &Service{
		serviceID:             cfg.ServiceID,
		nodeID:                nodeID,
		dir:                   dir,
		hwmFilePath:           filepath.Join(dir, hwmFile),
		clstr:                 clstr,
		in:                    in,
		logOnly:               cfg.LogOnly,
		endpoint:              cfg.Endpoint,
		httpClient:            httpClient,
		tlsConfig:             tlsConfig,
		transmitTimeout:       cfg.TransmitTimeout,
		transmitMaxRetries:    cfg.TransmitMaxRetries,
		transmitMinBackoff:    cfg.TransmitMinBackoff,
		transmitMaxBackoff:    cfg.TransmitMaxBackoff,
		transmitRetryPolicy:   cfg.TransmitRetryPolicy,
		maxBatchSz:            cfg.MaxBatchSz,
		maxBatchDelay:         cfg.MaxBatchDelay,
		highWatermarkInterval: cfg.HighWatermarkInterval,
		leaderObCh:            make(chan bool, leaderChanLen),
		hwmObCh:               make(chan uint64, leaderChanLen),
		done:                  make(chan struct{}),
		logger:                log.New(os.Stdout, "[cdc-service] ", log.LstdFlags),
	}
	srv.initBatcher()

	fifo, err := NewQueue(filepath.Join(dir, cdcDB))
	if err != nil {
		return nil, err
	}
	srv.fifo = fifo

	srv.highWatermark.Store(readHWMFromFile(srv.hwmFilePath))
	return srv, nil
}

// Start starts the CDC service.
func (s *Service) Start() error {
	s.wg.Add(2)
	go s.writeToFIFO()
	go s.mainLoop()

	s.clstr.RegisterLeaderChange(s.leaderObCh)
	s.clstr.RegisterHWMUpdate(s.hwmObCh)
	if s.serviceID == "" {
		s.logger.Printf("service started with node ID %s", s.nodeID)
	} else {
		s.logger.Printf("service started with ID %s, node ID %s", s.serviceID, s.nodeID)
	}
	return nil
}

// Stop stops the CDC service.
func (s *Service) Stop() {
	close(s.done)
	s.wg.Wait()
	s.fifo.Close()
}

// HighWatermark returns the high watermark of the CDC service. This
// is the index of the last event that was successfully sent to the webhook.
func (s *Service) HighWatermark() uint64 {
	return s.highWatermark.Load()
}

// IsLeader returns whether the CDC service is running on the Leader.
func (s *Service) IsLeader() bool {
	return s.isLeader.Is()
}

// Stats returns statistics about the CDC service.
func (s *Service) Stats() (map[string]any, error) {
	stats := map[string]any{
		"node_id":                  s.nodeID,
		"dir":                      s.dir,
		"current_highwater_mark":   s.HighWatermark(),
		"persisted_highwater_mark": readHWMFromFile(s.hwmFilePath),
		"is_leader":                s.IsLeader(),
		"endpoint":                 httpurl.RemoveBasicAuth(s.endpoint),
		"fifo": map[string]any{
			"has_next": s.fifo.HasNext(),
			"length":   s.fifo.Len(),
		},
	}
	if s.serviceID != "" {
		stats["service_id"] = s.serviceID
	}
	return stats, nil
}

func (s *Service) mainLoop() {
	defer s.wg.Done()

	var leaderStop, leaderDone chan struct{}
	var followerStop, followerDone chan struct{}

	// Helper function to stop leader loop
	stopLeaderLoop := func() {
		if leaderStop != nil {
			close(leaderStop)
			<-leaderDone
			leaderStop = nil
			leaderDone = nil
			s.initBatcher()
		}
	}

	// Helper function to stop follower loop
	stopFollowerLoop := func() {
		if followerStop != nil {
			close(followerStop)
			<-followerDone
			followerStop = nil
			followerDone = nil
		}
	}

	// Cleanup on exit
	defer func() {
		stopLeaderLoop()
		stopFollowerLoop()
	}()

	// Start in follower state.
	followerStop, followerDone = s.followerLoop()

	for {
		select {
		case leaderNow := <-s.leaderObCh:
			if leaderNow == s.isLeader.Is() {
				continue
			}
			s.isLeader.SetBool(leaderNow)
			if s.isLeader.Is() {
				s.logger.Printf("leadership changed, this node (ID:%s) now leader, starting CDC transmission",
					s.nodeID)
				stopFollowerLoop()
				leaderStop, leaderDone = s.leaderLoop()
			} else {
				s.logger.Printf("leadership changed, this node (ID:%s) no longer leader, pausing CDC transmission",
					s.nodeID)
				stopLeaderLoop()
				followerStop, followerDone = s.followerLoop()
			}

		case <-s.done:
			return
		}
	}
}

// leaderLoop handles CDC operations when this service is running on the leader.
// It reads from FIFO, processes batches, sends to HTTP endpoint, and broadcasts the high watermark.
func (s *Service) leaderLoop() (chan struct{}, chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		// Start reading from FIFO
		fifoStop, fifoDone := s.readFromFIFO()
		defer func() {
			if fifoStop != nil {
				close(fifoStop)
				<-fifoDone
			}
		}()

		// Start periodic high watermark update handling
		hwmStop, hwmDone := s.leaderHWMLoop()
		defer func() {
			if hwmStop != nil {
				close(hwmStop)
				<-hwmDone
			}
		}()

		for {
			select {
			case <-stop:
				return

			case batch := <-s.batcher.C:
				if batch == nil || len(batch.Objects) == 0 {
					continue
				}

				b, err := cdcjson.MarshalToEnvelopeJSON(s.serviceID, s.nodeID, false, batch.Objects)
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
			}
		}
	}()

	return stop, done
}

// leaderHWMLoop handles periodic high watermark operations for leaders.
// It broadcasts HWM to cluster, writes to disk, and prunes FIFO.
func (s *Service) leaderHWMLoop() (chan struct{}, chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		hwmTicker := time.NewTicker(s.highWatermarkInterval)
		defer hwmTicker.Stop()
		for {
			select {
			case <-stop:
				return

			case <-hwmTicker.C:
				hwm := s.highWatermark.Load()
				if err := s.clstr.BroadcastHighWatermark(hwm); err != nil {
					s.logger.Printf("error writing high watermark to store: %v", err)
				}
				if err := writeHWMToFile(s.hwmFilePath, hwm); err != nil {
					s.logger.Printf("error writing high watermark to file: %v", err)
				}
				if err := s.fifo.DeleteRange(hwm); err != nil {
					s.logger.Printf("error deleting events up to high watermark from FIFO: %v", err)
				}
				s.hwmLeaderUpdated.Add(1)
			}
		}
	}()

	return stop, done
}

// followerLoop handles CDC operations when this service is running on a follower.
func (s *Service) followerLoop() (chan struct{}, chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-stop:
				return
			case hwm := <-s.hwmObCh:
				// Handle high watermark updates from cluster
				s.highWatermark.Store(hwm)
				if err := writeHWMToFile(s.hwmFilePath, hwm); err != nil {
					s.logger.Printf("error writing high watermark to file: %v", err)
				}
				// This means all events up to this high watermark have been
				// successfully sent to the webhook by the cluster. We can
				// delete all events up and including that point from our FIFO.
				if err := s.fifo.DeleteRange(hwm); err != nil {
					s.logger.Printf("error deleting events up to high watermark from FIFO: %v", err)
				}
				s.hwmFollowerUpdated.Add(1)
			}
		}
	}()

	return stop, done
}

// writeToFIFO handles events sent to this service. It writes the events to the FIFO.
// Writing to the FIFO happens regardless of whether this service is running on
// the leader or a follower.
func (s *Service) writeToFIFO() {
	defer s.wg.Done()
	for {
		select {
		case o := <-s.in:
			b, err := command.MarshalCDCIndexedEventGroup(o)
			if err != nil {
				s.logger.Printf("error marshalling CDC events: %v", err)
				continue
			}
			if err := s.fifo.Enqueue(&Event{Index: o.Index, Data: b}); err != nil {
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
	go func() {
		for {
			select {
			case <-stop:
				close(done)
				return
			case ev := <-s.fifo.C:
				if ev == nil {
					close(done)
					return
				}
				events, err := command.UnmarshalCDCIndexedEventGroup(ev.Data)
				if err != nil {
					s.logger.Printf("error unmarshalling CDC events from FIFO: %v", err)
					continue
				}
				s.batcher.Write([]*proto.CDCIndexedEventGroup{events}, nil)
			}
		}
	}()
	return stop, done
}

func (s *Service) initBatcher() {
	if s.batcher != nil {
		s.batcher.Close()
		s.batcher = nil
	}
	s.batcher = queue.New[*proto.CDCIndexedEventGroup](s.maxBatchSz, s.maxBatchSz, s.maxBatchDelay)
}

// readHWMFromFile reads the high watermark from the specified file.
// Returns 0 if the file doesn't exist or if there's an error reading it.
func readHWMFromFile(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0 // File doesn't exist or can't be read
	}

	var hwm hwmData
	if err := json.Unmarshal(data, &hwm); err != nil {
		return 0 // Invalid JSON
	}

	return hwm.HWM
}

// writeHWMToFile writes the high watermark to the specified file with an fsync.
func writeHWMToFile(path string, hwm uint64) error {
	data := hwmData{
		HWM:       hwm,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Write to a temporary file first, then rename to ensure atomicity
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer os.Remove(tmpPath)

	_, err = file.Write(jsonData)
	if err != nil {
		file.Close()
		return err
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	// Atomically replace the old file
	return os.Rename(tmpPath, path)
}
