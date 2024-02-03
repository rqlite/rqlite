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
	"strings"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/auth"
	clstrPB "github.com/rqlite/rqlite/v8/cluster/proto"
	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/encoding"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/queue"
	"github.com/rqlite/rqlite/v8/rtls"
	"github.com/rqlite/rqlite/v8/store"
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
	Execute(er *proto.ExecuteRequest) ([]*proto.ExecuteResult, error)

	// Query executes a slice of queries, each of which returns rows. If
	// timings is true, then timing information will be returned. If tx
	// is true, then all queries will take place while a read transaction
	// is held on the database.
	Query(qr *proto.QueryRequest) ([]*proto.QueryRows, error)

	// Request processes a slice of requests, each of which can be either
	// an Execute or Query request.
	Request(eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, error)

	// Load loads a SQLite file into the system via Raft consensus.
	Load(lr *proto.LoadRequest) error
}

// Store is the interface the Raft-based database must implement.
type Store interface {
	Database

	// Remove removes the node from the cluster.
	Remove(rn *proto.RemoveNodeRequest) error

	// LeaderAddr returns the Raft address of the leader of the cluster.
	LeaderAddr() (string, error)

	// Ready returns whether the Store is ready to service requests.
	Ready() bool

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)

	// Nodes returns the slice of store.Servers in the cluster
	Nodes() ([]*store.Server, error)

	// Backup writes backup of the node state to dst
	Backup(br *proto.BackupRequest, dst io.Writer) error

	// ReadFrom reads and loads a SQLite database into the node, initially bypassing
	// the Raft system. It then triggers a Raft snapshot, which will then make
	// Raft aware of the new data.
	ReadFrom(r io.Reader) (int64, error)
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
	Execute(er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteResult, error)

	// Query performs an Query Request on a remote node.
	Query(qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) ([]*proto.QueryRows, error)

	// Request performs an ExecuteQuery Request on a remote node.
	Request(eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, error)

	// Backup retrieves a backup from a remote node and writes to the io.Writer.
	Backup(br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error

	// Load loads a SQLite database into the node.
	Load(lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error

	// RemoveNode removes a node from the cluster.
	RemoveNode(rn *proto.RemoveNodeRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error

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
	ExecuteResult        []*proto.ExecuteResult
	QueryRows            []*proto.QueryRows
	ExecuteQueryResponse []*proto.ExecuteQueryResponse

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
	numLoadAborted                    = "loads_aborted"
	numBoot                           = "boot"
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
	stats.Add(numLoadAborted, 0)
	stats.Add(numBoot, 0)
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
		mTLSState := rtls.MTLSStateDisabled
		if s.ClientVerify {
			mTLSState = rtls.MTLSStateEnabled
		}
		s.tlsConfig, err = rtls.CreateServerConfig(s.CertFile, s.KeyFile, s.CACertFile, mTLSState)
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

	params, err := NewQueryParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch {
	case r.URL.Path == "/" || r.URL.Path == "":
		http.Redirect(w, r, "/status", http.StatusFound)
	case strings.HasPrefix(r.URL.Path, "/db/execute"):
		stats.Add(numExecutions, 1)
		s.handleExecute(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/db/query"):
		stats.Add(numQueries, 1)
		s.handleQuery(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/db/request"):
		stats.Add(numRequests, 1)
		s.handleRequest(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/db/backup"):
		stats.Add(numBackups, 1)
		s.handleBackup(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/db/load"):
		stats.Add(numLoad, 1)
		s.handleLoad(w, r, params)
	case r.URL.Path == "/boot":
		stats.Add(numBoot, 1)
		s.handleBoot(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/remove"):
		s.handleRemove(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/status"):
		stats.Add(numStatus, 1)
		s.handleStatus(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/nodes"):
		s.handleNodes(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/readyz"):
		stats.Add(numReadyz, 1)
		s.handleReadyz(w, r, params)
	case r.URL.Path == "/debug/vars":
		s.handleExpvar(w, r, params)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		s.handlePprof(w, r, params)
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
func (s *Service) handleRemove(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	if !s.CheckRequestPerm(r, auth.PermRemove) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "DELETE" {
		w.WriteHeader(http.StatusMethodNotAllowed)
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

	rn := &proto.RemoveNodeRequest{
		Id: remoteID,
	}

	err = s.store.Remove(rn)
	if err != nil {
		if err == store.ErrNotLeader {
			if s.DoRedirect(w, r, qp) {
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
			removeErr := s.cluster.RemoveNode(rn, addr, makeCredentials(username, password), qp.Timeout(defaultTimeout))
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
func (s *Service) handleBackup(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	if !s.CheckRequestPerm(r, auth.PermBackup) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	br := &proto.BackupRequest{
		Format:   qp.BackupFormat(),
		Leader:   !qp.NoLeader(),
		Vacuum:   qp.Vacuum(),
		Compress: qp.Compress(),
	}
	addBackupFormatHeader(w, qp)

	err := s.store.Backup(br, w)
	if err != nil {
		if err == store.ErrNotLeader {
			if s.DoRedirect(w, r, qp) {
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
			backupErr := s.cluster.Backup(br, addr, makeCredentials(username, password), qp.Timeout(defaultTimeout), w)
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
func (s *Service) handleLoad(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	if !s.CheckRequestPerm(r, auth.PermLoad) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	resp := NewResponse()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	if db.IsValidSQLiteData(b) {
		s.logger.Printf("SQLite database file detected as load data")
		lr := &proto.LoadRequest{
			Data: b,
		}

		err := s.store.Load(lr)
		if err != nil && err != store.ErrNotLeader {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if err != nil && err == store.ErrNotLeader {
			if s.DoRedirect(w, r, qp) {
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
			loadErr := s.cluster.Load(lr, addr, makeCredentials(username, password),
				qp.Timeout(defaultTimeout), qp.Retries(0))
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
	} else {
		// No JSON structure expected for this API.
		queries := []string{string(b)}
		er := executeRequestFromStrings(queries, qp.Timings(), false)

		results, err := s.store.Execute(er)
		if err != nil {
			if err == store.ErrNotLeader {
				if s.DoRedirect(w, r, qp) {
					return
				}
			}
			resp.Error = err.Error()
		} else {
			resp.Results.ExecuteResult = results
		}
		resp.end = time.Now()
	}
	s.writeResponse(w, r, qp, resp)
}

// handleBoot handles booting this node using a SQLite file.
func (s *Service) handleBoot(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	if !s.CheckRequestPerm(r, auth.PermLoad) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	bufReader := bufio.NewReader(r.Body)
	peek, err := bufReader.Peek(db.SQLiteHeaderSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !db.IsValidSQLiteData(peek) {
		http.Error(w, "invalid SQLite data", http.StatusBadRequest)
		return
	}

	s.logger.Printf("starting boot process")
	_, err = s.store.ReadFrom(bufReader)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
}

// handleStatus returns status on the system.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request, qp QueryParams) {
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

	var b []byte
	if qp.Pretty() {
		b, err = json.MarshalIndent(status, "", "    ")
	} else {
		b, err = json.Marshal(status)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("JSON marshal: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	b, err = getSubJSON(b, qp.Key())
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
func (s *Service) handleNodes(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
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
	if !qp.NonVoters() {
		nodes = nodes.Voters()
	}

	// Now test the nodes
	lAddr, err := s.store.LeaderAddr()
	if err != nil {
		http.Error(w, fmt.Sprintf("leader address: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}
	nodes.Test(s.cluster, lAddr, qp.Timeout(defaultTimeout))

	enc := NewNodesRespEncoder(w, qp.Version() != "2")
	if qp.Pretty() {
		enc.SetIndent("", "    ")
	}
	err = enc.Encode(nodes)
	if err != nil {
		http.Error(w, fmt.Sprintf("JSON marshal: %s", err.Error()),
			http.StatusInternalServerError)
	}
}

// handleReadyz returns whether the node is ready.
func (s *Service) handleReadyz(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	if !s.CheckRequestPerm(r, auth.PermReady) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if qp.NoLeader() {
		// Simply handling the HTTP request is enough.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[+]node ok"))
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

	_, err = s.cluster.GetNodeAPIAddr(lAddr, qp.Timeout(defaultTimeout))
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

func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermExecute) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if qp.Queue() {
		stats.Add(numQueuedExecutions, 1)
		s.queuedExecute(w, r, qp)
	} else {
		s.execute(w, r, qp)
	}
}

// queuedExecute handles queued queries that modify the database.
func (s *Service) queuedExecute(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	resp := NewResponse()

	// Perform a leader check, unless disabled. This prevents generating queued writes on
	// a node that does not appear to be connected to a cluster (even a single-node cluster).
	if !qp.NoLeader() {
		addr, err := s.store.LeaderAddr()
		if err != nil || addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	stmts, err := ParseRequest(b)
	if err != nil {
		if errors.Is(err, ErrNoStatements) && !qp.Wait() {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if err := command.Rewrite(stmts, !qp.NoRewriteRandom()); err != nil {
		http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	var fc queue.FlushChannel
	if qp.Wait() {
		stats.Add(numQueuedExecutionsWait, 1)
		fc = make(queue.FlushChannel)
	}

	seqNum, err := s.stmtQueue.Write(stmts, fc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.SequenceNum = seqNum

	if qp.Wait() {
		// Wait for the flush channel to close, or timeout.
		select {
		case <-fc:
			break
		case <-time.NewTimer(qp.Timeout(defaultTimeout)).C:
			http.Error(w, "timeout", http.StatusRequestTimeout)
			return
		}
	}

	resp.end = time.Now()
	s.writeResponse(w, r, qp, resp)
}

// execute handles queries that modify the database.
func (s *Service) execute(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	resp := NewResponse()
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
	if err := command.Rewrite(stmts, !qp.NoRewriteRandom()); err != nil {
		http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction: qp.Tx(),
			DbTimeout:   int64(qp.DBTimeout(0)),
			Statements:  stmts,
		},
		Timings: qp.Timings(),
	}

	results, resultsErr := s.store.Execute(er)
	if resultsErr != nil && resultsErr == store.ErrNotLeader {
		if s.DoRedirect(w, r, qp) {
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
		results, resultsErr = s.cluster.Execute(er, addr, makeCredentials(username, password),
			qp.Timeout(defaultTimeout), qp.Retries(0))
		if resultsErr != nil {
			stats.Add(numRemoteExecutionsFailed, 1)
			if resultsErr.Error() == "unauthorized" {
				http.Error(w, "remote Execute not authorized", http.StatusUnauthorized)
				return
			}
			resultsErr = fmt.Errorf("node failed to process Execute on remote node at %s: %s",
				addr, resultsErr.Error())
		}
		stats.Add(numRemoteExecutions, 1)
	}

	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else {
		resp.Results.ExecuteResult = results
	}
	resp.end = time.Now()
	s.writeResponse(w, r, qp, resp)
}

// handleQuery handles queries that do not modify the database.
func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, auth.PermQuery) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get the query statement(s), and do tx if necessary.
	queries, err := requestQueries(r, qp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stats.Add(numQueryStmtsRx, int64(len(queries)))

	// No point rewriting queries if they don't go through the Raft log, since they
	// will never be replayed from the log anyway.
	if qp.Level() == proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG {
		if err := command.Rewrite(queries, qp.NoRewriteRandom()); err != nil {
			http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
			return
		}
	}

	resp := NewResponse()
	resp.Results.AssociativeJSON = qp.Associative()

	qr := &proto.QueryRequest{
		Request: &proto.Request{
			Transaction: qp.Tx(),
			DbTimeout:   int64(qp.DBTimeout(0)),
			Statements:  queries,
		},
		Timings:   qp.Timings(),
		Level:     qp.Level(),
		Freshness: qp.Freshness().Nanoseconds(),
	}

	results, resultsErr := s.store.Query(qr)
	if resultsErr != nil && resultsErr == store.ErrNotLeader {
		if s.DoRedirect(w, r, qp) {
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
		results, resultsErr = s.cluster.Query(qr, addr, makeCredentials(username, password), qp.Timeout(defaultTimeout))
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
	s.writeResponse(w, r, qp, resp)
}

func (s *Service) handleRequest(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPermAll(r, auth.PermQuery, auth.PermExecute) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
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

	if err := command.Rewrite(stmts, qp.NoRewriteRandom()); err != nil {
		http.Error(w, fmt.Sprintf("SQL rewrite: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	resp := NewResponse()
	resp.Results.AssociativeJSON = qp.Associative()

	eqr := &proto.ExecuteQueryRequest{
		Request: &proto.Request{
			Transaction: qp.Tx(),
			Statements:  stmts,
			DbTimeout:   int64(qp.DBTimeout(0)),
		},
		Timings:   qp.Timings(),
		Level:     qp.Level(),
		Freshness: qp.Freshness().Nanoseconds(),
	}

	results, resultsErr := s.store.Request(eqr)
	if resultsErr != nil && resultsErr == store.ErrNotLeader {
		if s.DoRedirect(w, r, qp) {
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
		results, resultsErr = s.cluster.Request(eqr, addr, makeCredentials(username, password),
			qp.Timeout(defaultTimeout), qp.Retries(0))
		if resultsErr != nil {
			stats.Add(numRemoteRequestsFailed, 1)
			if resultsErr.Error() == "unauthorized" {
				http.Error(w, "remote Request not authorized", http.StatusUnauthorized)
				return
			}
			resultsErr = fmt.Errorf("node failed to process Request on remote node at %s: %s",
				addr, resultsErr.Error())
		}
		stats.Add(numRemoteRequests, 1)
	}

	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else {
		resp.Results.ExecuteQueryResponse = results
	}
	resp.end = time.Now()
	s.writeResponse(w, r, qp, resp)
}

// handleExpvar serves registered expvar information over HTTP.
func (s *Service) handleExpvar(w http.ResponseWriter, r *http.Request, qp QueryParams) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if !s.CheckRequestPerm(r, auth.PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if qp.Key() != "" && qp.Key() != kv.Key {
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
func (s *Service) handlePprof(w http.ResponseWriter, r *http.Request, qp QueryParams) {
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

// DoRedirect checks if the request is a redirect, and if so, performs the redirect.
// Returns true caller can consider the request handled. Returns false if the request
// was not a redirect and the caller should continue processing the request.
func (s *Service) DoRedirect(w http.ResponseWriter, r *http.Request, qp QueryParams) bool {
	if !qp.Redirect() {
		return false
	}

	rd, err := s.FormRedirect(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		http.Redirect(w, r, rd, http.StatusMovedPermanently)
	}
	return true
}

// FormRedirect returns the value for the "Location" header for a 301 response.
func (s *Service) FormRedirect(r *http.Request) (string, error) {
	leaderAPIAddr := s.LeaderAPIAddr()
	if leaderAPIAddr == "" {
		stats.Add(numLeaderNotFound, 1)
		return "", ErrLeaderNotFound
	}

	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}
	return fmt.Sprintf("%s%s%s", leaderAPIAddr, r.URL.Path, rq), nil
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
			er := &proto.ExecuteRequest{
				Request: &proto.Request{
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
							_, err = s.cluster.Execute(er, addr, nil, defaultTimeout, 0)
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

// addBackupFormatHeader adds the Content-Type header for the backup format.
func addBackupFormatHeader(w http.ResponseWriter, qp QueryParams) {
	w.Header().Set("Content-Type", "application/octet-stream")
	if qp.BackupFormat() == proto.BackupRequest_BACKUP_REQUEST_FORMAT_SQL {
		w.Header().Set("Content-Type", "application/sql")
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
func (s *Service) writeResponse(w http.ResponseWriter, r *http.Request, qp QueryParams, j Responser) {
	var b []byte
	var err error
	if qp.Timings() {
		j.SetTime()
	}

	if qp.Pretty() {
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

func requestQueries(r *http.Request, qp QueryParams) ([]*proto.Statement, error) {
	if r.Method == "GET" {
		return []*proto.Statement{
			{
				Sql: qp.Query(),
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

func prettyEnabled(e bool) string {
	if e {
		return "enabled"
	}
	return "disabled"
}

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
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

func makeCredentials(username, password string) *clstrPB.Credentials {
	return &clstrPB.Credentials{
		Username: username,
		Password: password,
	}
}
