// Package http provides the HTTP server for accessing the distributed database.
package http

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rqlite/rqlite/auth"
	"github.com/rqlite/rqlite/cluster"
	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/chunking"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/queue"
	"github.com/rqlite/rqlite/rtls"
	"github.com/rqlite/rqlite/store"
)

const (
	defaultChunkSize = 5 * 1024 * 1024 // 5 MB
)

var (
	// ErrLeaderNotFound is returned when a node cannot locate a leader
	ErrLeaderNotFound = errors.New("leader not found")
)

type ResultsError interface {
	Error() string
	IsAuthorized() bool
}

// Database is the interface any queryable system must implement
type Database interface {
	// Execute executes a slice of queries, each of which is not expected
	// to return rows. If timings is true, then timing information will
	// be return. If tx is true, then either all queries will be executed
	// successfully or it will as though none executed.
	Execute(er *command.ExecuteRequest) ([]*command.ExecuteResult, error)

	// Query executes a slice of queries, each of which returns rows. If
	// timings is true, then timing information will be returned. If tx
	// is true, then all queries will take place while a read transaction
	// is held on the database.
	Query(qr *command.QueryRequest) ([]*command.QueryRows, error)

	// Request processes a slice of requests, each of which can be either
	// an Execute or Query request.
	Request(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, error)

	// LoadChunk loads a SQLite database into the node, chunk by chunk.
	LoadChunk(lc *command.LoadChunkRequest) error
}

// Store is the interface the Raft-based database must implement.
type Store interface {
	Database

	// Remove removes the node from the cluster.
	Remove(rn *command.RemoveNodeRequest) error

	// LeaderAddr returns the Raft address of the leader of the cluster.
	LeaderAddr() (string, error)

	// Ready returns whether the Store is ready to service requests.
	Ready() bool

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)

	// Nodes returns the slice of store.Servers in the cluster
	Nodes() ([]*store.Server, error)

	// Backup writes backup of the node state to dst
	Backup(br *command.BackupRequest, dst io.Writer) error
}

// GetAddresser is the interface that wraps the GetNodeAPIAddr method.
// GetNodeAPIAddr returns the HTTP API URL for the node at the given Raft address.
type GetAddresser interface {
	GetNodeAPIAddr(addr string, timeout time.Duration) (string, error)
}

// Cluster is the interface node API services must provide
type Cluster interface {
	GetAddresser

	// Execute performs an Execute Request on a remote node.
	Execute(er *command.ExecuteRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration) ([]*command.ExecuteResult, error)

	// Query performs an Query Request on a remote node.
	Query(qr *command.QueryRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration) ([]*command.QueryRows, error)

	// Request performs an ExecuteQuery Request on a remote node.
	Request(eqr *command.ExecuteQueryRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration) ([]*command.ExecuteQueryResponse, error)

	// Backup retrieves a backup from a remote node and writes to the io.Writer.
	Backup(br *command.BackupRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration, w io.Writer) error

	// LoadChunk loads a SQLite database into the node, chunk by chunk.
	LoadChunk(lc *command.LoadChunkRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration) error

	// RemoveNode removes a node from the cluster.
	RemoveNode(rn *command.RemoveNodeRequest, nodeAddr string, creds *cluster.Credentials, timeout time.Duration) error

	// Stats returns stats on the Cluster.
	Stats() (map[string]interface{}, error)
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// AA authenticates and checks authorization for the given perm.
	AA(username, password, perm string) bool
}

// StatusReporter is the interface status providers must implement.
type StatusReporter interface {
	Stats() (map[string]interface{}, error)
}

// DBResults stores either an Execute result, a Query result, or
// an ExecuteQuery result.
type DBResults struct {
	ExecuteResult        []*command.ExecuteResult
	QueryRows            []*command.QueryRows
	ExecuteQueryResponse []*command.ExecuteQueryResponse

	AssociativeJSON bool // Render in associative form
}

// Responser is the interface response objects must implement.
type Responser interface {
	SetTime()
}

// MarshalJSON implements the JSON Marshaler interface.
func (d *DBResults) MarshalJSON() ([]byte, error) {
	enc := encoding.Encoder{
		Associative: d.AssociativeJSON,
	}

	if d.ExecuteResult != nil {
		return enc.JSONMarshal(d.ExecuteResult)
	} else if d.QueryRows != nil {
		return enc.JSONMarshal(d.QueryRows)
	} else if d.ExecuteQueryResponse != nil {
		return enc.JSONMarshal(d.ExecuteQueryResponse)
	}
	return json.Marshal(make([]interface{}, 0))
}

// Response represents a response from the HTTP service.
type Response struct {
	Results     *DBResults `json:"results,omitempty"`
	Error       string     `json:"error,omitempty"`
	Time        float64    `json:"time,omitempty"`
	SequenceNum int64      `json:"sequence_number,omitempty"`

	start time.Time
	end   time.Time
}

// SetTime sets the Time attribute of the response. This way it will be present
// in the serialized JSON version.
func (r *Response) SetTime() {
	r.Time = r.end.Sub(r.start).Seconds()
}

// NewResponse returns a new instance of response.
func NewResponse() *Response {
	return &Response{
		Results: &DBResults{},
		start:   time.Now(),
	}
}

// stats captures stats for the HTTP service.
var stats *expvar.Map

const (
	numLeaderNotFound                 = "leader_not_found"
	numExecutions                     = "executions"
	numExecuteStmtsRx                 = "execute_stmts_rx"
	numQueuedExecutions               = "queued_executions"
	numQueuedExecutionsOK             = "queued_executions_ok"
	numQueuedExecutionsStmtsRx        = "queued_executions_num_stmts_rx"
	numQueuedExecutionsStmtsTx        = "queued_executions_num_stmts_tx"
	numQueuedExecutionsNoLeader       = "queued_executions_no_leader"
	numQueuedExecutionsNotLeader      = "queued_executions_not_leader"
	numQueuedExecutionsLeadershipLost = "queued_executions_leadership_lost"
	numQueuedExecutionsUnknownError   = "queued_executions_unknown_error"
	numQueuedExecutionsFailed         = "queued_executions_failed"
	numQueuedExecutionsWait           = "queued_executions_wait"
	numQueries                        = "queries"
	numQueryStmtsRx                   = "query_stmts_rx"
	numRequests                       = "requests"
	numRequestStmtsRx                 = "request_stmts_rx"
	numRemoteExecutions               = "remote_executions"
	numRemoteExecutionsFailed         = "remote_executions_failed"
	numRemoteQueries                  = "remote_queries"
	numRemoteQueriesFailed            = "remote_queries_failed"
	numRemoteRequests                 = "remote_requests"
	numRemoteRequestsFailed           = "remote_requests_failed"
	numRemoteBackups                  = "remote_backups"
	numRemoteLoads                    = "remote_loads"
	numRemoteRemoveNode               = "remote_remove_node"
	numReadyz                         = "num_readyz"
	numStatus                         = "num_status"
	numBackups                        = "backups"
	numLoad                           = "loads"
	numAuthOK                         = "authOK"
	numAuthFail                       = "authFail"

	// Default timeout for cluster communications.
	defaultTimeout = 30 * time.Second

	// VersionHTTPHeader is the HTTP header key for the version.
	VersionHTTPHeader = "X-RQLITE-VERSION"

	// ServedByHTTPHeader is the HTTP header used to report which
	// node (by node Raft address) actually served the request if
	// it wasn't served by this node.
	ServedByHTTPHeader = "X-RQLITE-SERVED-BY"

	// AllowOriginHeader is the HTTP header for allowing CORS compliant access from certain origins
	AllowOriginHeader = "Access-Control-Allow-Origin"

	// AllowMethodsHeader is the HTTP header for supporting the correct methods
	AllowMethodsHeader = "Access-Control-Allow-Methods"

	// AllowHeadersHeader is the HTTP header for supporting the correct request headers
	AllowHeadersHeader = "Access-Control-Allow-Headers"

	// AllowCredentialsHeader is the HTTP header for supporting specifying credentials
	AllowCredentialsHeader = "Access-Control-Allow-Credentials"
)

func init() {
	stats = expvar.NewMap("http")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numLeaderNotFound, 0)
	stats.Add(numExecutions, 0)
	stats.Add(numExecuteStmtsRx, 0)
	stats.Add(numQueuedExecutions, 0)
	stats.Add(numQueuedExecutionsOK, 0)
	stats.Add(numQueuedExecutionsStmtsRx, 0)
	stats.Add(numQueuedExecutionsStmtsTx, 0)
	stats.Add(numQueuedExecutionsNoLeader, 0)
	stats.Add(numQueuedExecutionsNotLeader, 0)
	stats.Add(numQueuedExecutionsLeadershipLost, 0)
	stats.Add(numQueuedExecutionsUnknownError, 0)
	stats.Add(numQueuedExecutionsFailed, 0)
	stats.Add(numQueuedExecutionsWait, 0)
	stats.Add(numQueries, 0)
	stats.Add(numQueryStmtsRx, 0)
	stats.Add(numRequests, 0)
	stats.Add(numRequestStmtsRx, 0)
	stats.Add(numRemoteExecutions, 0)
	stats.Add(numRemoteExecutionsFailed, 0)
	stats.Add(numRemoteQueries, 0)
	stats.Add(numRemoteQueriesFailed, 0)
	stats.Add(numRemoteRequests, 0)
	stats.Add(numRemoteRequestsFailed, 0)
	stats.Add(numRemoteBackups, 0)
	stats.Add(numRemoteLoads, 0)
	stats.Add(numRemoteRemoveNode, 0)
	stats.Add(numReadyz, 0)
	stats.Add(numStatus, 0)
	stats.Add(numBackups, 0)
	stats.Add(numLoad, 0)
	stats.Add(numAuthOK, 0)
	stats.Add(numAuthFail, 0)
}

// Service provides HTTP service.
type Service struct {
	httpServer http.Server
	closeCh    chan struct{}
	addr       string       // Bind address of the HTTP service.
	ln         net.Listener // Service listener

	store Store // The Raft-backed database store.

	queueDone chan struct{}
	stmtQueue *queue.Queue // Queue for queued executes

	cluster Cluster // The Cluster service.

	start      time.Time // Start up time.
	lastBackup time.Time // Time of last successful backup.

	statusMu sync.RWMutex
	statuses map[string]StatusReporter

	CACertFile   string // Path to x509 CA certificate used to verify certificates.
	CertFile     string // Path to server's own x509 certificate.
	KeyFile      string // Path to server's own x509 private key.
	ClientVerify bool   // Whether client certificates should verified.
	tlsConfig    *tls.Config

	AllowOrigin string // Value to set for Access-Control-Allow-Origin

	DefaultQueueCap     int
	DefaultQueueBatchSz int
	DefaultQueueTimeout time.Duration
	DefaultQueueTx      bool

	seqNumMu sync.Mutex
	seqNum   int64 // Last sequence number written OK.

	credentialStore CredentialStore

	BuildInfo map[string]interface{}

	logger *log.Logger
}

// New returns an uninitialized HTTP service. If credentials is nil, then
// the service performs no authentication and authorization checks.
func New(addr string, store Store, cluster Cluster, credentials CredentialStore) *Service {
	return &Service{
		addr:                addr,
		store:               store,
		DefaultQueueCap:     1024,
		DefaultQueueBatchSz: 128,
		DefaultQueueTimeout: 100 * time.Millisecond,
		cluster:             cluster,
		start:               time.Now(),
		statuses:            make(map[string]StatusReporter),
		credentialStore:     credentials,
		logger:              log.New(os.Stderr, "[http] ", log.LstdFlags),
	}
}

// Start starts the service.
func (s *Service) Start() error {
	s.httpServer = http.Server{
		Handler: s,
	}

	var ln net.Listener
	var err error
	if s.CertFile == "" || s.KeyFile == "" {
		ln, err = net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}
	} else {
		s.tlsConfig, err = rtls.CreateServerConfig(s.CertFile, s.KeyFile, s.CACertFile, !s.ClientVerify)
		if err != nil {
			return err
		}
		ln, err = tls.Listen("tcp", s.addr, s.tlsConfig)
		if err != nil {
			return err
		}
		var b strings.Builder
		b.WriteString(fmt.Sprintf("secure HTTPS server enabled with cert %s, key %s", s.CertFile, s.KeyFile))
		if s.CACertFile != "" {
			b.WriteString(fmt.Sprintf(", CA cert %s", s.CACertFile))
		}
		if s.ClientVerify {
			b.WriteString(", mutual TLS enabled")
		} else {
			b.WriteString(", mutual TLS disabled")
		}
		// print the message
		s.logger.Println(b.String())
	}
	s.ln = ln

	s.closeCh = make(chan struct{})
	s.queueDone = make(chan struct{})

	s.stmtQueue = queue.New(s.DefaultQueueCap, s.DefaultQueueBatchSz, s.DefaultQueueTimeout)
	go s.runQueue()
	s.logger.Printf("execute queue processing started with capacity %d, batch size %d, timeout %s",
		s.DefaultQueueCap, s.DefaultQueueBatchSz, s.DefaultQueueTimeout.String())

	go func() {
		err := s.httpServer.Serve(s.ln)
		if err != nil {
			s.logger.Printf("HTTP service on %s stopped: %s", s.ln.Addr().String(), err.Error())
		}
	}()
	s.logger.Println("service listening on", s.Addr())

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.logger.Println("closing HTTP service on", s.ln.Addr().String())
	s.httpServer.Shutdown(context.Background())

	s.stmtQueue.Close()
	select {
	case <-s.queueDone:
	default:
		close(s.closeCh)
	}
	<-s.queueDone

	s.ln.Close()
}

// HTTPS returns whether this service is using HTTPS.
func (s *Service) HTTPS() bool {
	return s.CertFile != "" && s.KeyFile != ""
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.addBuildVersion(w)
	s.addAllowHeaders(w)

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	switch {
	case r.URL.Path == "/" || r.URL.Path == "":
		http.Redirect(w, r, "/status", http.StatusFound)
	case strings.HasPrefix(r.URL.Path, "/db/execute"):
		stats.Add(numExecutions, 1)
		s.handleExecute(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/query"):
		stats.Add(numQueries, 1)
		s.handleQuery(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/request"):
		stats.Add(numRequests, 1)
		s.handleRequest(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/backup"):
		stats.Add(numBackups, 1)
		s.handleBackup(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/load"):
		stats.Add(numLoad, 1)
		s.handleLoad(w, r)
	case strings.HasPrefix(r.URL.Path, "/remove"):
		s.handleRemove(w, r)
	case strings.HasPrefix(r.URL.Path, "/status"):
		stats.Add(numStatus, 1)
		s.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/nodes"):
		s.handleNodes(w, r)
	case strings.HasPrefix(r.URL.Path, "/readyz"):
		stats.Add(numReadyz, 1)
		s.handleReadyz(w, r)
	case r.URL.Path == "/debug/vars":
		s.handleExpvar(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		s.handlePprof(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// RegisterStatus allows other modules to register status for serving over HTTP.
func (s *Service) RegisterStatus(key string, stat StatusReporter) error {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if _, ok := s.statuses[key]; ok {
		return fmt.Errorf("status already registered with key %s", key)
	}
	s.statuses[key] = stat

	return nil
}

// handleRemove handles cluster-remove requests.
func (s *Service) handleRemove(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, auth.PermRemove) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "DELETE" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	redirect, err := isRedirect(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteID, ok := m["id"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	timeout, err := timeoutParam(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rn := &command.RemoveNodeRequest{
		Id: remoteID,
	}

	err = s.store.Remove(rn)
	if err != nil {
		if err == store.ErrNotLeader {
			if redirect {
				leaderAPIAddr := s.LeaderAPIAddr()
				if leaderAPIAddr == "" {
					stats.Add(numLeaderNotFound, 1)
					http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
					return
				}

				redirect := s.FormRedirect(r, leaderAPIAddr)
				http.Redirect(w, r, redirect, http.StatusMovedPermanently)
				return
			}

			addr, err := s.store.LeaderAddr()
			if err != nil {
				http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
					http.StatusInternalServerError)
				return
			}
			if addr == "" {
				stats.Add(numLeaderNotFound, 1)
				http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
				return
			}

			username, password, ok := r.BasicAuth()
			if !ok {
				username = ""
			}

			w.Header().Add(ServedByHTTPHeader, addr)
			removeErr := s.cluster.RemoveNode(rn, addr, makeCredentials(username, password), timeout)
			if removeErr != nil {
				if removeErr.Error() == "unauthorized" {
					http.Error(w, "remote remove node not authorized", http.StatusUnauthorized)
				} else {
					http.Error(w, removeErr.Error(), http.StatusInternalServerError)
				}
				return
			}
			stats.Add(numRemoteRemoveNode, 1)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleBackup returns the consistent database snapshot.
func (s *Service) handleBackup(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, auth.PermBackup) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	noLeader, err := noLeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirect, err := isRedirect(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	format, err := backupFormat(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	vacuum, err := isVacuum(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	timeout, err := timeoutParam(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	br := &command.BackupRequest{
		Format: format,
		Leader: !noLeader,
		Vacuum: vacuum,
	}

	err = s.store.Backup(br, w)
	if err != nil {
		if err == store.ErrNotLeader {
			if redirect {
				leaderAPIAddr := s.LeaderAPIAddr()
				if leaderAPIAddr == "" {
					stats.Add(numLeaderNotFound, 1)
					http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
					return
				}

				redirect := s.FormRedirect(r, leaderAPIAddr)
				http.Redirect(w, r, redirect, http.StatusMovedPermanently)
				return
			}

			addr, err := s.store.LeaderAddr()
			if err != nil {
				http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
					http.StatusInternalServerError)
				return
			}
			if addr == "" {
				stats.Add(numLeaderNotFound, 1)
				http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
				return
			}

			username, password, ok := r.BasicAuth()
			if !ok {
				username = ""
			}

			w.Header().Add(ServedByHTTPHeader, addr)
			backupErr := s.cluster.Backup(br, addr, makeCredentials(username, password), timeout, w)
			if backupErr != nil {
				if backupErr.Error() == "unauthorized" {
					http.Error(w, "remote backup not authorized", http.StatusUnauthorized)
				} else {
					http.Error(w, backupErr.Error(), http.StatusInternalServerError)
				}
				return
			}
			stats.Add(numRemoteBackups, 1)
			return
		} else if err == store.ErrInvalidVacuum {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.lastBackup = time.Now()
}

// handleLoad loads the database from the given SQLite database file or SQLite dump.
func (s *Service) handleLoad(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	if !s.CheckRequestPerm(r, auth.PermLoad) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	resp := NewResponse()

	timings, err := isTimings(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	timeout, err := timeoutParam(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	redirect, err := isRedirect(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	chunkSz, err := chunkSizeParam(r, defaultChunkSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Peek at the incoming bytes so we can determine if this is a SQLite database
	validSQLite := false
	bufReader := bufio.NewReader(r.Body)
	peek, err := bufReader.Peek(db.SQLiteHeaderSize)
	if err == nil {
		validSQLite = db.IsValidSQLiteData(peek)
		if validSQLite {
			s.logger.Printf("SQLite database file detected as load data")
			if db.IsWALModeEnabled(peek) {
				s.logger.Printf("SQLite database file is in WAL mode - rejecting load request")
				http.Error(w, `SQLite database file is in WAL mode - convert it to DELETE mode via 'PRAGMA journal_mode=DELETE'`,
					http.StatusBadRequest)
				return
			}
		}

	}

	if !validSQLite {
		// Assume SQL text
		b, err := io.ReadAll(bufReader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.Body.Close()

		queries := []string{string(b)}
		er := executeRequestFromStrings(queries, timings, false)

		results, err := s.store.Execute(er)
		if err != nil {
			if err == store.ErrNotLeader {
				leaderAPIAddr := s.LeaderAPIAddr()
				if leaderAPIAddr == "" {
					stats.Add(numLeaderNotFound, 1)
					http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
					return
				}

				redirect := s.FormRedirect(r, leaderAPIAddr)
				http.Redirect(w, r, redirect, http.StatusMovedPermanently)
				return
			}
			resp.Error = err.Error()
		} else {
			resp.Results.ExecuteResult = results
		}
		resp.end = time.Now()
	} else {
		chunker := chunking.NewChunker(bufReader, int64(chunkSz))
		for {
			chunk, err := chunker.Next()
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			err = s.store.LoadChunk(chunk)
			if err != nil && err != store.ErrNotLeader {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if err != nil && err == store.ErrNotLeader {
				if redirect {
					leaderAPIAddr := s.LeaderAPIAddr()
					if leaderAPIAddr == "" {
						stats.Add(numLeaderNotFound, 1)
						http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
						return
					}

					redirect := s.FormRedirect(r, leaderAPIAddr)
					http.Redirect(w, r, redirect, http.StatusMovedPermanently)
					return
				}

				addr, err := s.store.LeaderAddr()
				if err != nil {
					http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
						http.StatusInternalServerError)
					return
				}
				if addr == "" {
					stats.Add(numLeaderNotFound, 1)
					http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
					return
				}

				username, password, ok := r.BasicAuth()
				if !ok {
					username = ""
				}

				w.Header().Add(ServedByHTTPHeader, addr)
				loadErr := s.cluster.LoadChunk(chunk, addr, makeCredentials(username, password), timeout)
				if loadErr != nil {
					if loadErr.Error() == "unauthorized" {
						http.Error(w, "remote load not authorized", http.StatusUnauthorized)
					} else {
						http.Error(w, loadErr.Error(), http.StatusInternalServerError)
					}
					return
				}
				stats.Add(numRemoteLoads, 1)
				// Allow this if block to exit, so response remains as before request
				// forwarding was put in place.
			}
			if chunk.IsLast {
				nChunks, nr, nw := chunker.Counts()
				s.logger.Printf("%d bytes read, %d chunks generated, containing %d bytes of compressed data (compression ratio %.2f)",
					nr, nChunks, nw, float64(nr)/float64(nw))
				break
			}
		}
	}

	s.logger.Printf("load request completed in %s", time.Since(startTime).String())
	s.writeResponse(w, r, resp)
}

// handleStatus returns status on the system.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	storeStatus, err := s.store.Stats()
	if err != nil {
		http.Error(w, fmt.Sprintf("store stats: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	clusterStatus, err := s.cluster.Stats()
	if err != nil {
		http.Error(w, fmt.Sprintf("cluster stats: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	rt := map[string]interface{}{
		"GOARCH":        runtime.GOARCH,
		"GOOS":          runtime.GOOS,
		"GOMAXPROCS":    runtime.GOMAXPROCS(0),
		"num_cpu":       runtime.NumCPU(),
		"num_goroutine": runtime.NumGoroutine(),
		"version":       runtime.Version(),
	}

	oss := map[string]interface{}{
		"pid":       os.Getpid(),
		"ppid":      os.Getppid(),
		"page_size": os.Getpagesize(),
	}
	executable, err := os.Executable()
	if err == nil {
		oss["executable"] = executable
	}
	hostname, err := os.Hostname()
	if err == nil {
		oss["hostname"] = hostname
	}

	qs, err := s.stmtQueue.Stats()
	if err != nil {
		http.Error(w, fmt.Sprintf("queue stats: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}
	s.seqNumMu.Lock()
	qs["sequence_number"] = s.seqNum
	s.seqNumMu.Unlock()
	queueStats := map[string]interface{}{
		"_default": qs,
	}
	httpStatus := map[string]interface{}{
		"bind_addr": s.Addr().String(),
		"auth":      prettyEnabled(s.credentialStore != nil),
		"cluster":   clusterStatus,
		"queue":     queueStats,
		"tls":       s.tlsStats(),
	}

	nodeStatus := map[string]interface{}{
		"start_time":   s.start,
		"current_time": time.Now(),
		"uptime":       time.Since(s.start).String(),
	}

	// Build the status response.
	status := map[string]interface{}{
		"os":      oss,
		"runtime": rt,
		"store":   storeStatus,
		"http":    httpStatus,
		"node":    nodeStatus,
	}
	if !s.lastBackup.IsZero() {
		status["last_backup_time"] = s.lastBackup
	}
	if s.BuildInfo != nil {
		status["build"] = s.BuildInfo
	}

	// Add any registered StatusReporters.
	func() {
		s.statusMu.RLock()
		defer s.statusMu.RUnlock()
		for k, v := range s.statuses {
			stat, err := v.Stats()
			if err != nil {
				http.Error(w, fmt.Sprintf("registered stats: %s", err.Error()),
					http.StatusInternalServerError)
				return
			}
			status[k] = stat
		}
	}()

	pretty, _ := isPretty(r)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(status, "", "    ")
	} else {
		b, err = json.Marshal(status)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("JSON marshal: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	key := keyParam(r)
	b, err = getSubJSON(b, key)
	if err != nil {
		http.Error(w, fmt.Sprintf("JSON subkey: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	_, err = w.Write(b)
	if err != nil {
		http.Error(w, fmt.Sprintf("write: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}
}

// handleNodes returns status on the other voting nodes in the system.
// This attempts to contact all the nodes in the cluster, so may take
// some time to return.
func (s *Service) handleNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	timeout, err := timeoutParam(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	includeNonVoters, err := nonVoters(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get nodes in the cluster, and possibly filter out non-voters.
	sNodes, err := s.store.Nodes()
	if err != nil {
		statusCode := http.StatusInternalServerError
		if err == store.ErrNotOpen {
			statusCode = http.StatusServiceUnavailable
		}
		http.Error(w, fmt.Sprintf("store nodes: %s", err.Error()), statusCode)
		return
	}
	nodes := NewNodesFromServers(sNodes)
	if !includeNonVoters {
		nodes = nodes.Voters()
	}

	// Now test the nodes
	lAddr, err := s.store.LeaderAddr()
	if err != nil {
		http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}
	nodes.Test(s.cluster, lAddr, timeout)

	ver, err := verParam(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	enc := NewNodesRespEncoder(w, ver != "2")
	pretty, _ := isPretty(r)
	if pretty {
		enc.SetIndent("", "    ")
	}
	err = enc.Encode(nodes)
	if err != nil {
		http.Error(w, fmt.Sprintf("JSON marshal: %s", err.Error()),
			http.StatusInternalServerError)
	}
}

// handleReadyz returns whether the node is ready.
func (s *Service) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, auth.PermReady) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	noLeader, err := noLeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if noLeader {
		// Simply handling the HTTP request is enough.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[+]node ok"))
		return
	}

	timeout, err := timeoutParam(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	lAddr, err := s.store.LeaderAddr()
	if err != nil {
		http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	if lAddr == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("[+]node ok\n[+]leader does not exist"))
		return
	}

	_, err = s.cluster.GetNodeAPIAddr(lAddr, timeout)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf("[+]node ok\n[+]leader not contactable: %s", err.Error())))
		return
	}

	if !s.store.Ready() {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("[+]node ok\n[+]leader ok\n[+]store not ready"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("[+]node ok\n[+]leader ok\n[+]store ok"))
}

func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermExecute) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	queue, err := isQueue(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if queue {
		stats.Add(numQueuedExecutions, 1)
		s.queuedExecute(w, r)
	} else {
		s.execute(w, r)
	}
}

// queuedExecute handles queued queries that modify the database.
func (s *Service) queuedExecute(w http.ResponseWriter, r *http.Request) {
	resp := NewResponse()

	// Perform a leader check, unless disabled. This prevents generating queued writes on
	// a node that does not appear to be connected to a cluster (even a single-node cluster).
	noLeader, err := noLeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !noLeader {
		addr, err := s.store.LeaderAddr()
		if err != nil || addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	wait, err := isWait(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	stmts, err := ParseRequest(b)
	if err != nil {
		if errors.Is(err, ErrNoStatements) && !wait {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	noRewriteRandom, err := noRewriteRandom(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := command.Rewrite(stmts, !noRewriteRandom); err != nil {
		http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	timeout, err := timeoutParam(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var fc queue.FlushChannel
	if wait {
		stats.Add(numQueuedExecutionsWait, 1)
		fc = make(queue.FlushChannel)
	}

	seqNum, err := s.stmtQueue.Write(stmts, fc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.SequenceNum = seqNum

	if wait {
		// Wait for the flush channel to close, or timeout.
		select {
		case <-fc:
			break
		case <-time.NewTimer(timeout).C:
			http.Error(w, "timeout", http.StatusRequestTimeout)
			return
		}
	}

	resp.end = time.Now()
	s.writeResponse(w, r, resp)
}

// execute handles queries that modify the database.
func (s *Service) execute(w http.ResponseWriter, r *http.Request) {
	resp := NewResponse()

	timeout, isTx, timings, redirect, noRewriteRandom, err := reqParams(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	stmts, err := ParseRequest(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats.Add(numExecuteStmtsRx, int64(len(stmts)))
	if err := command.Rewrite(stmts, !noRewriteRandom); err != nil {
		http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	er := &command.ExecuteRequest{
		Request: &command.Request{
			Transaction: isTx,
			Statements:  stmts,
		},
		Timings: timings,
	}

	results, resultsErr := s.store.Execute(er)
	if resultsErr != nil && resultsErr == store.ErrNotLeader {
		if redirect {
			leaderAPIAddr := s.LeaderAPIAddr()
			if leaderAPIAddr == "" {
				stats.Add(numLeaderNotFound, 1)
				http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
				return
			}
			loc := s.FormRedirect(r, leaderAPIAddr)
			http.Redirect(w, r, loc, http.StatusMovedPermanently)
			return
		}

		addr, err := s.store.LeaderAddr()
		if err != nil {
			http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
				http.StatusInternalServerError)
			return
		}
		if addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
			return
		}

		username, password, ok := r.BasicAuth()
		if !ok {
			username = ""
		}

		w.Header().Add(ServedByHTTPHeader, addr)
		results, resultsErr = s.cluster.Execute(er, addr, makeCredentials(username, password), timeout)
		if resultsErr != nil {
			stats.Add(numRemoteExecutionsFailed, 1)
			if resultsErr.Error() == "unauthorized" {
				http.Error(w, "remote execute not authorized", http.StatusUnauthorized)
				return
			}
		}
		stats.Add(numRemoteExecutions, 1)
	}

	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else {
		resp.Results.ExecuteResult = results
	}
	resp.end = time.Now()
	s.writeResponse(w, r, resp)
}

// handleQuery handles queries that do not modify the database.
func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermQuery) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	timeout, frsh, lvl, isTx, timings, redirect, noRewriteRandom, isAssoc, err := queryReqParams(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get the query statement(s), and do tx if necessary.
	queries, err := requestQueries(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats.Add(numQueryStmtsRx, int64(len(queries)))

	// No point rewriting queries if they don't go through the Raft log, since they
	// will never be replayed from the log anyway.
	if lvl == command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG {
		if err := command.Rewrite(queries, noRewriteRandom); err != nil {
			http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
			return
		}
	}

	resp := NewResponse()
	resp.Results.AssociativeJSON = isAssoc

	qr := &command.QueryRequest{
		Request: &command.Request{
			Transaction: isTx,
			Statements:  queries,
		},
		Timings:   timings,
		Level:     lvl,
		Freshness: frsh.Nanoseconds(),
	}

	results, resultsErr := s.store.Query(qr)
	if resultsErr != nil && resultsErr == store.ErrNotLeader {
		if redirect {
			leaderAPIAddr := s.LeaderAPIAddr()
			if leaderAPIAddr == "" {
				stats.Add(numLeaderNotFound, 1)
				http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
				return
			}
			loc := s.FormRedirect(r, leaderAPIAddr)
			http.Redirect(w, r, loc, http.StatusMovedPermanently)
			return
		}

		addr, err := s.store.LeaderAddr()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
			return
		}
		username, password, ok := r.BasicAuth()
		if !ok {
			username = ""
		}

		w.Header().Add(ServedByHTTPHeader, addr)
		results, resultsErr = s.cluster.Query(qr, addr, makeCredentials(username, password), timeout)
		if resultsErr != nil {
			stats.Add(numRemoteQueriesFailed, 1)
			if resultsErr.Error() == "unauthorized" {
				http.Error(w, "remote query not authorized", http.StatusUnauthorized)
				return
			}
		}
		stats.Add(numRemoteQueries, 1)
	}

	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else {
		resp.Results.QueryRows = results
	}
	resp.end = time.Now()
	s.writeResponse(w, r, resp)
}

func (s *Service) handleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPermAll(r, auth.PermQuery, auth.PermExecute) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	timeout, frsh, lvl, isTx, timings, redirect, noRewriteRandom, isAssoc, err := executeQueryReqParams(r, defaultTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	stmts, err := ParseRequest(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats.Add(numRequestStmtsRx, int64(len(stmts)))

	if err := command.Rewrite(stmts, noRewriteRandom); err != nil {
		http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	resp := NewResponse()
	resp.Results.AssociativeJSON = isAssoc

	eqr := &command.ExecuteQueryRequest{
		Request: &command.Request{
			Transaction: isTx,
			Statements:  stmts,
		},
		Timings:   timings,
		Level:     lvl,
		Freshness: frsh.Nanoseconds(),
	}

	results, resultErr := s.store.Request(eqr)
	if resultErr != nil && resultErr == store.ErrNotLeader {
		if redirect {
			leaderAPIAddr := s.LeaderAPIAddr()
			if leaderAPIAddr == "" {
				stats.Add(numLeaderNotFound, 1)
				http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
				return
			}
			loc := s.FormRedirect(r, leaderAPIAddr)
			http.Redirect(w, r, loc, http.StatusMovedPermanently)
			return
		}

		addr, err := s.store.LeaderAddr()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
			return
		}
		username, password, ok := r.BasicAuth()
		if !ok {
			username = ""
		}

		w.Header().Add(ServedByHTTPHeader, addr)
		results, resultErr = s.cluster.Request(eqr, addr, makeCredentials(username, password), timeout)
		if resultErr != nil {
			stats.Add(numRemoteRequestsFailed, 1)
			if resultErr.Error() == "unauthorized" {
				http.Error(w, "remote request not authorized", http.StatusUnauthorized)
				return
			}
		}
		stats.Add(numRemoteRequests, 1)
	}

	if resultErr != nil {
		resp.Error = resultErr.Error()
	} else {
		resp.Results.ExecuteQueryResponse = results
	}
	resp.end = time.Now()
	s.writeResponse(w, r, resp)
}

// handleExpvar serves registered expvar information over HTTP.
func (s *Service) handleExpvar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	key := keyParam(r)

	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if key != "" && key != kv.Key {
			return
		}
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// handlePprof serves pprof information over HTTP.
func (s *Service) handlePprof(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch r.URL.Path {
	case "/debug/pprof/cmdline":
		pprof.Cmdline(w, r)
	case "/debug/pprof/profile":
		pprof.Profile(w, r)
	case "/debug/pprof/symbol":
		pprof.Symbol(w, r)
	default:
		pprof.Index(w, r)
	}
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

// FormRedirect returns the value for the "Location" header for a 301 response.
func (s *Service) FormRedirect(r *http.Request, url string) string {
	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}
	return fmt.Sprintf("%s%s%s", url, r.URL.Path, rq)
}

// CheckRequestPerm checks if the request is authenticated and authorized
// with the given Perm.
func (s *Service) CheckRequestPerm(r *http.Request, perm string) (b bool) {
	defer func() {
		if b {
			stats.Add(numAuthOK, 1)
		} else {
			stats.Add(numAuthFail, 1)
		}
	}()

	// No auth store set, so no checking required.
	if s.credentialStore == nil {
		return true
	}

	username, password, ok := r.BasicAuth()
	if !ok {
		username = ""
	}

	return s.credentialStore.AA(username, password, perm)
}

// CheckRequestPermAll checksif the request is authenticated and authorized
// with all the given Perms.
func (s *Service) CheckRequestPermAll(r *http.Request, perms ...string) (b bool) {
	defer func() {
		if b {
			stats.Add(numAuthOK, 1)
		} else {
			stats.Add(numAuthFail, 1)
		}
	}()

	// No auth store set, so no checking required.
	if s.credentialStore == nil {
		return true
	}

	username, password, ok := r.BasicAuth()
	if !ok {
		username = ""
	}

	for _, perm := range perms {
		if !s.credentialStore.AA(username, password, perm) {
			return false
		}
	}
	return true
}

// LeaderAPIAddr returns the API address of the leader, as known by this node.
func (s *Service) LeaderAPIAddr() string {
	nodeAddr, err := s.store.LeaderAddr()
	if err != nil {
		return ""
	}

	apiAddr, err := s.cluster.GetNodeAPIAddr(nodeAddr, defaultTimeout)

	if err != nil {
		return ""
	}
	return apiAddr
}

func (s *Service) runQueue() {
	defer close(s.queueDone)
	retryDelay := time.Second

	var err error
	for {
		select {
		case <-s.closeCh:
			return
		case req := <-s.stmtQueue.C:
			er := &command.ExecuteRequest{
				Request: &command.Request{
					Statements:  req.Statements,
					Transaction: s.DefaultQueueTx,
				},
			}
			stats.Add(numQueuedExecutionsStmtsRx, int64(len(req.Statements)))

			// Nil statements are valid, as clients may want to just send
			// a "checkpoint" through the queue.
			if er.Request.Statements != nil {
				for {
					_, err = s.store.Execute(er)
					if err == nil {
						// Success!
						break
					}

					if err == store.ErrNotLeader {
						addr, err := s.store.LeaderAddr()
						if err != nil || addr == "" {
							s.logger.Printf("execute queue can't find leader for sequence number %d on node %s",
								req.SequenceNumber, s.Addr().String())
							stats.Add(numQueuedExecutionsNoLeader, 1)
						} else {
							_, err = s.cluster.Execute(er, addr, nil, defaultTimeout)
							if err != nil {
								s.logger.Printf("execute queue write failed for sequence number %d on node %s: %s",
									req.SequenceNumber, s.Addr().String(), err.Error())
								if err.Error() == "leadership lost while committing log" {
									stats.Add(numQueuedExecutionsLeadershipLost, 1)
								} else if err.Error() == "not leader" {
									stats.Add(numQueuedExecutionsNotLeader, 1)
								} else {
									stats.Add(numQueuedExecutionsUnknownError, 1)
								}
							} else {
								// Success!
								stats.Add(numRemoteExecutions, 1)
								break
							}
						}
					}

					stats.Add(numQueuedExecutionsFailed, 1)
					time.Sleep(retryDelay)
				}
			}

			// Perform post-write processing.
			s.seqNumMu.Lock()
			s.seqNum = req.SequenceNumber
			s.seqNumMu.Unlock()
			req.Close()
			stats.Add(numQueuedExecutionsStmtsTx, int64(len(req.Statements)))
			stats.Add(numQueuedExecutionsOK, 1)
		}
	}
}

// addBuildVersion adds the build version to the HTTP response.
func (s *Service) addBuildVersion(w http.ResponseWriter) {
	// Add version header to every response, if available.
	version := "unknown"
	if v, ok := s.BuildInfo["version"].(string); ok {
		version = v
	}
	w.Header().Add(VersionHTTPHeader, version)
}

// addAllowHeaders adds the Access-Control-Allow-Origin, Access-Control-Allow-Methods,
// and Access-Control-Allow-Headers headers to the HTTP response.
func (s *Service) addAllowHeaders(w http.ResponseWriter) {
	if s.AllowOrigin != "" {
		w.Header().Add(AllowOriginHeader, s.AllowOrigin)
	}
	w.Header().Add(AllowMethodsHeader, "OPTIONS, GET, POST")
	if s.credentialStore == nil {
		w.Header().Add(AllowHeadersHeader, "Content-Type")
	} else {
		w.Header().Add(AllowHeadersHeader, "Content-Type, Authorization")
		w.Header().Add(AllowCredentialsHeader, "true")
	}
}

// tlsStats returns the TLS stats for the service.
func (s *Service) tlsStats() map[string]interface{} {
	m := map[string]interface{}{
		"enabled": fmt.Sprintf("%t", s.tlsConfig != nil),
	}
	if s.tlsConfig != nil {
		m["client_auth"] = s.tlsConfig.ClientAuth.String()
		m["cert_file"] = s.CertFile
		m["key_file"] = s.KeyFile
		m["ca_file"] = s.CACertFile
		m["next_protos"] = s.tlsConfig.NextProtos
	}
	return m
}

// writeResponse writes the given response to the given writer.
func (s *Service) writeResponse(w http.ResponseWriter, r *http.Request, j Responser) {
	var b []byte
	var err error
	pretty, _ := isPretty(r)
	timings, _ := isTimings(r)

	if timings {
		j.SetTime()
	}

	if pretty {
		b, err = json.MarshalIndent(j, "", "    ")
	} else {
		b, err = json.Marshal(j)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(b)
	if err != nil {
		s.logger.Println("writing response failed:", err.Error())
	}
}

func requestQueries(r *http.Request) ([]*command.Statement, error) {
	if r.Method == "GET" {
		query, err := stmtParam(r)
		if err != nil || query == "" {
			return nil, errors.New("bad query GET request")
		}
		return []*command.Statement{
			{
				Sql: query,
			},
		}, nil
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errors.New("bad query POST request")
	}
	r.Body.Close()

	return ParseRequest(b)
}

// queryParam returns whether the given query param is present.
func queryParam(req *http.Request, param string) (bool, error) {
	err := req.ParseForm()
	if err != nil {
		return false, err
	}
	if _, ok := req.Form[param]; ok {
		return true, nil
	}
	return false, nil
}

// stmtParam returns the value for URL param 'q', if present.
func stmtParam(req *http.Request) (string, error) {
	q := req.URL.Query()
	stmt := strings.TrimSpace(q.Get("q"))
	return stmt, nil
}

// fmtParam returns the value for URL param 'fmt', if present.
func fmtParam(req *http.Request) (string, error) {
	q := req.URL.Query()
	return strings.TrimSpace(q.Get("fmt")), nil
}

// verParam returns the requested version, if present.
func verParam(req *http.Request) (string, error) {
	q := req.URL.Query()
	return strings.TrimSpace(q.Get("ver")), nil
}

// isPretty returns whether the HTTP response body should be pretty-printed.
func isPretty(req *http.Request) (bool, error) {
	return queryParam(req, "pretty")
}

// isVacuum returns whether the HTTP request is requesting a vacuum.
func isVacuum(req *http.Request) (bool, error) {
	return queryParam(req, "vacuum")
}

// isRedirect returns whether the HTTP request is requesting a explicit
// redirect to the leader, if necessary.
func isRedirect(req *http.Request) (bool, error) {
	return queryParam(req, "redirect")
}

func keyParam(req *http.Request) string {
	q := req.URL.Query()
	return strings.TrimSpace(q.Get("key"))
}

func getSubJSON(jsonBlob []byte, keyString string) (json.RawMessage, error) {
	if keyString == "" {
		return jsonBlob, nil
	}

	keys := strings.Split(keyString, ".")
	var obj interface{}
	if err := json.Unmarshal(jsonBlob, &obj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	for _, key := range keys {
		switch val := obj.(type) {
		case map[string]interface{}:
			if value, ok := val[key]; ok {
				obj = value
			} else {
				emptyObj := json.RawMessage("{}")
				return emptyObj, nil
			}
		default:
			// If a value is not a map, marshal and return this value
			finalObjBytes, err := json.Marshal(obj)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal final object: %w", err)
			}
			return finalObjBytes, nil
		}
	}

	finalObjBytes, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal final object: %w", err)
	}

	return finalObjBytes, nil
}

// timeoutParam returns the value, if any, set for timeout. If not set, it
// returns the value passed in as a default.
func timeoutParam(req *http.Request, def time.Duration) (time.Duration, error) {
	q := req.URL.Query()
	timeout := strings.TrimSpace(q.Get("timeout"))
	if timeout == "" {
		return def, nil
	}
	t, err := time.ParseDuration(timeout)
	if err != nil {
		return 0, err
	}
	return t, nil
}

func chunkSizeParam(req *http.Request, defSz int) (int, error) {
	q := req.URL.Query()
	chunkSize := strings.TrimSpace(q.Get("chunk_kb"))
	if chunkSize == "" {
		return defSz, nil
	}
	sz, err := strconv.Atoi(chunkSize)
	if err != nil {
		return defSz, nil
	}
	return sz * 1024, nil
}

// isTx returns whether the HTTP request is requesting a transaction.
func isTx(req *http.Request) (bool, error) {
	return queryParam(req, "transaction")
}

// isQueue returns whether the HTTP request is requesting a queue.
func isQueue(req *http.Request) (bool, error) {
	return queryParam(req, "queue")
}

// reqParams is a convenience function to get a bunch of query params
// in one function call.
func reqParams(req *http.Request, def time.Duration) (timeout time.Duration, tx, timings, redirect, noRwRandom bool, err error) {
	timeout, err = timeoutParam(req, def)
	if err != nil {
		return 0, false, false, false, true, err
	}
	tx, err = isTx(req)
	if err != nil {
		return 0, false, false, false, true, err
	}
	timings, err = isTimings(req)
	if err != nil {
		return 0, false, false, false, true, err
	}
	redirect, err = isRedirect(req)
	if err != nil {
		return 0, false, false, false, true, err
	}
	noRwRandom, err = noRewriteRandom(req)
	if err != nil {
		return 0, false, false, false, true, err
	}
	return timeout, tx, timings, redirect, noRwRandom, nil
}

// queryReqParams is a convenience function to get a bunch of query params
// in one function call.
func queryReqParams(req *http.Request, def time.Duration) (timeout, frsh time.Duration, lvl command.QueryRequest_Level, isTx, timings, redirect, noRwRandom, isAssoc bool, err error) {
	timeout, isTx, timings, redirect, noRwRandom, err = reqParams(req, defaultTimeout)
	if err != nil {
		return 0, 0, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false, false, false, false, err
	}

	lvl, err = level(req)
	if err != nil {
		return 0, 0, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false, false, false, false, err
	}

	frsh, err = freshness(req)
	if err != nil {
		return 0, 0, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false, false, false, false, err
	}

	isAssoc, err = isAssociative(req)
	if err != nil {
		return 0, 0, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false, false, false, false, err
	}
	return
}

func executeQueryReqParams(req *http.Request, def time.Duration) (timeout, frsh time.Duration, lvl command.QueryRequest_Level, isTx, timings, redirect, noRwRandom, isAssoc bool, err error) {
	timeout, frsh, lvl, isTx, timings, redirect, noRwRandom, isAssoc, err = queryReqParams(req, defaultTimeout)
	if err != nil {
		return 0, 0, command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, false, false, false, false, false, err
	}
	return timeout, frsh, lvl, isTx, timings, redirect, noRwRandom, isAssoc, nil
}

// noLeader returns whether processing should skip the leader check.
func noLeader(req *http.Request) (bool, error) {
	return queryParam(req, "noleader")
}

// nonVoters returns whether a query is requesting to include non-voter results
func nonVoters(req *http.Request) (bool, error) {
	return queryParam(req, "nonvoters")
}

// isTimings returns whether timings are requested.
func isTimings(req *http.Request) (bool, error) {
	return queryParam(req, "timings")
}

// isWait returns whether a wait operation is requested.
func isWait(req *http.Request) (bool, error) {
	return queryParam(req, "wait")
}

func isAssociative(req *http.Request) (bool, error) {
	return queryParam(req, "associative")
}

// noRewriteRandom returns whether a rewrite of RANDOM is disabled.
func noRewriteRandom(req *http.Request) (bool, error) {
	return queryParam(req, "norwrandom")
}

// level returns the requested consistency level for a query
func level(req *http.Request) (command.QueryRequest_Level, error) {
	q := req.URL.Query()
	lvl := strings.TrimSpace(q.Get("level"))

	switch strings.ToLower(lvl) {
	case "none":
		return command.QueryRequest_QUERY_REQUEST_LEVEL_NONE, nil
	case "weak":
		return command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, nil
	case "strong":
		return command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG, nil
	default:
		return command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK, nil
	}
}

// freshness returns any freshness requested with a query.
func freshness(req *http.Request) (time.Duration, error) {
	q := req.URL.Query()
	f := strings.TrimSpace(q.Get("freshness"))
	if f == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(f)
	if err != nil {
		return 0, err
	}
	return d, nil
}

// backupFormat returns the request backup format, setting the response header
// accordingly.
func backupFormat(w http.ResponseWriter, r *http.Request) (command.BackupRequest_Format, error) {
	fmt, err := fmtParam(r)
	if err != nil {
		return command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY, err
	}
	if fmt == "sql" {
		w.Header().Set("Content-Type", "application/sql")
		return command.BackupRequest_BACKUP_REQUEST_FORMAT_SQL, nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	return command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY, nil
}

func prettyEnabled(e bool) string {
	if e {
		return "enabled"
	}
	return "disabled"
}

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
func executeRequestFromStrings(s []string, timings, tx bool) *command.ExecuteRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}

	}
	return &command.ExecuteRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}

func makeCredentials(username, password string) *cluster.Credentials {
	return &cluster.Credentials{
		Username: username,
		Password: password,
	}
}
