// Package http provides the HTTP server for accessing the distributed database.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/encoding"
	"github.com/rqlite/rqlite/store"
)

var (
	// ErrLeaderNotFound is returned when a node cannot locate a leader
	ErrLeaderNotFound = errors.New("leader not found")
)

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
}

// Store is the interface the Raft-based database must implement.
type Store interface {
	Database

	// Join joins the node with the given ID, reachable at addr, to this node.
	Join(id, addr string, voter bool) error

	// Remove removes the node, specified by id, from the cluster.
	Remove(id string) error

	// LeaderAddr returns the Raft address of the leader of the cluster.
	LeaderAddr() (string, error)

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)

	// Nodes returns the slice of store.Servers in the cluster
	Nodes() ([]*store.Server, error)

	// Backup wites backup of the node state to dst
	Backup(leader bool, f store.BackupFormat, dst io.Writer) error
}

// Cluster is the interface node API services must provide
type Cluster interface {
	// GetNodeAPIAddr returns the HTTP API URL for the node at the given Raft address.
	GetNodeAPIAddr(nodeAddr string, timeout time.Duration) (string, error)

	// Execute performs an Execute Request on a remote node.
	Execute(er *command.ExecuteRequest, nodeAddr string, timeout time.Duration) ([]*command.ExecuteResult, error)

	// Query performs an Query Request on a remote node.
	Query(qr *command.QueryRequest, nodeAddr string, timeout time.Duration) ([]*command.QueryRows, error)

	// Stats returns stats on the Cluster.
	Stats() (map[string]interface{}, error)
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// Check returns whether username and password are a valid combination.
	Check(username, password string) bool

	// HasPerm returns whether username has the given perm.
	HasPerm(username string, perm string) bool

	// HasAnyPerm returns whether username has any of the given perms.
	HasAnyPerm(username string, perm ...string) bool
}

// Statuser is the interface status providers must implement.
type Statuser interface {
	Stats() (map[string]interface{}, error)
}

// DBResults stores either an Execute result or a Query result
type DBResults struct {
	ExecuteResult []*command.ExecuteResult
	QueryRows     []*command.QueryRows
}

// MarshalJSON implements the JSON Marshaler interface.
func (d *DBResults) MarshalJSON() ([]byte, error) {
	if d.ExecuteResult != nil {
		return encoding.JSONMarshal(d.ExecuteResult)
	} else if d.QueryRows != nil {
		return encoding.JSONMarshal(d.QueryRows)
	}
	return nil, fmt.Errorf("no DB results set")
}

// Response represents a response from the HTTP service.
type Response struct {
	Results *DBResults `json:"results,omitempty"`
	Error   string     `json:"error,omitempty"`
	Time    float64    `json:"time,omitempty"`

	start time.Time
	end   time.Time
}

// stats captures stats for the HTTP service.
var stats *expvar.Map

const (
	numLeaderNotFound   = "leader_not_found"
	numExecutions       = "executions"
	numQueries          = "queries"
	numRemoteExecutions = "remote_executions"
	numRemoteQueries    = "remote_queries"
	numBackups          = "backups"
	numLoad             = "loads"
	numJoins            = "joins"
	numAuthOK           = "authOK"
	numAuthFail         = "authFail"

	// Default timeout for cluster communications.
	defaulTimeout = 30 * time.Second

	// PermAll means all actions permitted.
	PermAll = "all"
	// PermJoin means user is permitted to join cluster.
	PermJoin = "join"
	// PermRemove means user is permitted to remove a node.
	PermRemove = "remove"
	// PermExecute means user can access execute endpoint.
	PermExecute = "execute"
	// PermQuery means user can access query endpoint
	PermQuery = "query"
	// PermStatus means user can retrieve node status.
	PermStatus = "status"
	// PermBackup means user can backup node.
	PermBackup = "backup"
	// PermLoad means user can load a SQLite dump into a node.
	PermLoad = "load"

	// VersionHTTPHeader is the HTTP header key for the version.
	VersionHTTPHeader = "X-RQLITE-VERSION"

	// ServedByHTTPHeader is the HTTP header used to report which
	// node (by node Raft address) actually served the request if
	// it wasn't served by this node.
	ServedByHTTPHeader = "X-RQLITE-SERVED-BY"
)

func init() {
	stats = expvar.NewMap("http")
	stats.Add(numLeaderNotFound, 0)
	stats.Add(numExecutions, 0)
	stats.Add(numQueries, 0)
	stats.Add(numRemoteExecutions, 0)
	stats.Add(numRemoteQueries, 0)
	stats.Add(numBackups, 0)
	stats.Add(numLoad, 0)
	stats.Add(numJoins, 0)
	stats.Add(numAuthOK, 0)
	stats.Add(numAuthFail, 0)
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

// Service provides HTTP service.
type Service struct {
	addr string       // Bind address of the HTTP service.
	ln   net.Listener // Service listener

	store Store // The Raft-backed database store.

	cluster Cluster // The Cluster service.

	start      time.Time // Start up time.
	lastBackup time.Time // Time of last successful backup.

	statusMu sync.RWMutex
	statuses map[string]Statuser

	CACertFile string // Path to root X.509 certificate.
	CertFile   string // Path to SSL certificate.
	KeyFile    string // Path to SSL private key.
	TLS1011    bool   // Whether older, deprecated TLS should be supported.

	credentialStore CredentialStore

	Expvar bool
	Pprof  bool

	BuildInfo map[string]interface{}

	logger *log.Logger
}

// New returns an uninitialized HTTP service. If credentials is nil, then
// the service performs no authentication and authorization checks.
func New(addr string, store Store, cluster Cluster, credentials CredentialStore) *Service {
	return &Service{
		addr:            addr,
		store:           store,
		cluster:         cluster,
		start:           time.Now(),
		statuses:        make(map[string]Statuser),
		credentialStore: credentials,
		logger:          log.New(os.Stderr, "[http] ", log.LstdFlags),
	}
}

// Start starts the service.
func (s *Service) Start() error {
	server := http.Server{
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
		config, err := createTLSConfig(s.CertFile, s.KeyFile, s.CACertFile, s.TLS1011)
		if err != nil {
			return err
		}
		ln, err = tls.Listen("tcp", s.addr, config)
		if err != nil {
			return err
		}
		s.logger.Printf("secure HTTPS server enabled with cert %s, key %s", s.CertFile, s.KeyFile)
	}
	s.ln = ln

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			s.logger.Println("HTTP service Serve() returned:", err.Error())
		}
	}()
	s.logger.Println("service listening on", s.Addr())

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.ln.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.addBuildVersion(w)
	if !s.checkCredentials(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch {
	case strings.HasPrefix(r.URL.Path, "/db/execute"):
		stats.Add(numExecutions, 1)
		s.handleExecute(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/query"):
		stats.Add(numQueries, 1)
		s.handleQuery(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/backup"):
		stats.Add(numBackups, 1)
		s.handleBackup(w, r)
	case strings.HasPrefix(r.URL.Path, "/db/load"):
		stats.Add(numLoad, 1)
		s.handleLoad(w, r)
	case strings.HasPrefix(r.URL.Path, "/join"):
		stats.Add(numJoins, 1)
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "/remove"):
		s.handleRemove(w, r)
	case strings.HasPrefix(r.URL.Path, "/status"):
		s.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/nodes"):
		s.handleNodes(w, r)
	case r.URL.Path == "/debug/vars" && s.Expvar:
		s.handleExpvar(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof") && s.Pprof:
		s.handlePprof(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// RegisterStatus allows other modules to register status for serving over HTTP.
func (s *Service) RegisterStatus(key string, stat Statuser) error {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if _, ok := s.statuses[key]; ok {
		return fmt.Errorf("status already registered with key %s", key)
	}
	s.statuses[key] = stat

	return nil
}

// handleJoin handles cluster-join requests from other nodes.
func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, PermJoin) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	md := map[string]interface{}{}
	if err := json.Unmarshal(b, &md); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteID, ok := md["id"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := md["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	voter, ok := md["voter"]
	if !ok {
		voter = true
	}

	if err := s.store.Join(remoteID.(string), remoteAddr.(string), voter.(bool)); err != nil {
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

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleRemove handles cluster-remove requests.
func (s *Service) handleRemove(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, PermRemove) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "DELETE" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
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

	if err := s.store.Remove(remoteID); err != nil {
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

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleBackup returns the consistent database snapshot.
func (s *Service) handleBackup(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, PermBackup) {
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

	bf, err := backupFormat(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = s.store.Backup(!noLeader, bf, w)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.lastBackup = time.Now()
}

// handleLoad loads the state contained in a .dump output. This API is different
// from others in that it expects a raw file, not wrapped in any kind of JSON.
func (s *Service) handleLoad(w http.ResponseWriter, r *http.Request) {
	if !s.CheckRequestPerm(r, PermLoad) {
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

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// No JSON structure expected for this API.
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
	s.writeResponse(w, r, resp)
}

// handleStatus returns status on the system.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	storeStatus, err := s.store.Stats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	clusterStatus, err := s.cluster.Stats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

	httpStatus := map[string]interface{}{
		"bind_addr": s.Addr().String(),
		"auth":      prettyEnabled(s.credentialStore != nil),
		"cluster":   clusterStatus,
	}

	nodeStatus := map[string]interface{}{
		"start_time": s.start,
		"uptime":     time.Since(s.start).String(),
	}

	// Build the status response.
	status := map[string]interface{}{
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

	// Add any registered statusers.
	func() {
		s.statusMu.RLock()
		defer s.statusMu.RUnlock()
		for k, v := range s.statuses {
			stat, err := v.Stats()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// handleNodes returns status on the other voting nodes in the system.
// This attempts to contact all the nodes in the cluster, so may take
// some time to return.
func (s *Service) handleNodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	t, err := timeout(r, time.Duration(1))
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
	nodes, err := s.store.Nodes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filteredNodes := make([]*store.Server, 0)
	for _, n := range nodes {
		if n.Suffrage != "Voter" && !includeNonVoters {
			continue
		}
		filteredNodes = append(filteredNodes, n)
	}

	lAddr, err := s.store.LeaderAddr()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	nodesResp, err := s.checkNodes(filteredNodes, t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := make(map[string]struct {
		APIAddr   string  `json:"api_addr,omitempty"`
		Addr      string  `json:"addr,omitempty"`
		Reachable bool    `json:"reachable"`
		Leader    bool    `json:"leader"`
		Time      float64 `json:"time,omitempty"`
		Error     string  `json:"error,omitempty"`
	})

	for _, n := range filteredNodes {
		nn := resp[n.ID]
		nn.Addr = n.Addr
		nn.Leader = nn.Addr == lAddr
		nn.APIAddr = nodesResp[n.ID].apiAddr
		nn.Reachable = nodesResp[n.ID].reachable
		nn.Time = nodesResp[n.ID].time.Seconds()
		nn.Error = nodesResp[n.ID].error
		resp[n.ID] = nn
	}

	pretty, _ := isPretty(r)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// handleExecute handles queries that modify the database.
func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if !s.CheckRequestPerm(r, PermExecute) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	resp := NewResponse()

	timeout, isTx, timings, redirect, err := reqParams(r, defaulTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		if addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
		}
		results, resultsErr = s.cluster.Execute(er, addr, timeout)
		stats.Add(numRemoteExecutions, 1)
		w.Header().Add(ServedByHTTPHeader, addr)
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

	if !s.CheckRequestPerm(r, PermQuery) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	resp := NewResponse()

	timeout, isTx, timings, redirect, err := reqParams(r, defaulTimeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	lvl, err := level(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	frsh, err := freshness(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get the query statement(s), and do tx if necessary.
	queries, err := requestQueries(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

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
		}
		if addr == "" {
			stats.Add(numLeaderNotFound, 1)
			http.Error(w, ErrLeaderNotFound.Error(), http.StatusServiceUnavailable)
		}
		results, resultsErr = s.cluster.Query(qr, addr, timeout)
		stats.Add(numRemoteQueries, 1)
		w.Header().Add(ServedByHTTPHeader, addr)
	}

	if resultsErr != nil {
		resp.Error = resultsErr.Error()
	} else {
		resp.Results.QueryRows = results
	}
	resp.end = time.Now()
	s.writeResponse(w, r, resp)
}

// handleExpvar serves registered expvar information over HTTP.
func (s *Service) handleExpvar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if !s.CheckRequestPerm(r, PermStatus) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
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
	if !s.CheckRequestPerm(r, PermStatus) {
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

// CheckRequestPerm returns true if authentication is enabled and the user contained
// in the BasicAuth request has either PermAll, or the given perm.
func (s *Service) CheckRequestPerm(r *http.Request, perm string) (b bool) {
	defer func() {
		if b {
			stats.Add(numAuthOK, 1)
		} else {
			stats.Add(numAuthFail, 1)
		}
	}()
	if s.credentialStore == nil {
		return true
	}

	username, _, ok := r.BasicAuth()
	if !ok {
		return false
	}
	return s.credentialStore.HasAnyPerm(username, perm, PermAll)
}

// LeaderAPIAddr returns the API address of the leader, as known by this node.
func (s *Service) LeaderAPIAddr() string {
	nodeAddr, err := s.store.LeaderAddr()
	if err != nil {
		return ""
	}

	apiAddr, err := s.cluster.GetNodeAPIAddr(nodeAddr, defaulTimeout)

	if err != nil {
		return ""
	}
	return apiAddr
}

type checkNodesResponse struct {
	apiAddr   string
	reachable bool
	time      time.Duration
	error     string
}

// checkNodes returns a map of node ID to node responsivness, reachable
// being defined as node responds to a simple request over the network.
func (s *Service) checkNodes(nodes []*store.Server, timeout time.Duration) (map[string]*checkNodesResponse, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	resp := make(map[string]*checkNodesResponse)

	for _, n := range nodes {
		resp[n.ID] = &checkNodesResponse{}
	}

	// Now confirm.
	for _, n := range nodes {
		wg.Add(1)
		go func(id, raftAddr string) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()

			start := time.Now()
			apiAddr, err := s.cluster.GetNodeAPIAddr(raftAddr, defaulTimeout)
			if err != nil {
				resp[id].error = err.Error()
				return
			}
			resp[id].reachable = true
			resp[id].apiAddr = apiAddr
			resp[id].time = time.Since(start)
		}(n.ID, n.Addr)
	}
	wg.Wait()

	return resp, nil
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

// checkCredentials returns if any authentication requirements
// have been successfully met.
func (s *Service) checkCredentials(r *http.Request) bool {
	if s.credentialStore == nil {
		return true
	}

	username, password, ok := r.BasicAuth()
	return ok && s.credentialStore.Check(username, password)
}

// writeResponse writes the given response to the given writer.
func (s *Service) writeResponse(w http.ResponseWriter, r *http.Request, j *Response) {
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

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, errors.New("bad query POST request")
	}
	r.Body.Close()

	return ParseRequest(b)
}

// createTLSConfig returns a TLS config from the given cert and key.
func createTLSConfig(certFile, keyFile, caCertFile string, tls1011 bool) (*tls.Config, error) {
	var err error

	var minTls = uint16(tls.VersionTLS12)
	if tls1011 {
		minTls = tls.VersionTLS10
	}

	config := &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
		MinVersion: minTls,
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	if caCertFile != "" {
		asn1Data, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = x509.NewCertPool()
		ok := config.RootCAs.AppendCertsFromPEM([]byte(asn1Data))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate(s) in %q", caCertFile)
		}
	}
	return config, nil
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

// isPretty returns whether the HTTP response body should be pretty-printed.
func isPretty(req *http.Request) (bool, error) {
	return queryParam(req, "pretty")
}

// isRedirect returns whether the HTTP request is requesting a explicit
// redirect to the leader, if necessary.
func isRedirect(req *http.Request) (bool, error) {
	return queryParam(req, "redirect")
}

// timeoutParam returns the value, if any, set for timeout. If not set,
// it returns the value passed in as a default.
func timeoutParam(req *http.Request, def time.Duration) (time.Duration, error) {
	q := req.URL.Query()
	timeout := strings.TrimSpace(q.Get("timeout"))
	if timeout == "" {
		return def, nil
	}
	d, err := time.ParseDuration(timeout)
	if err != nil {
		return 0, err
	}
	return d, nil
}

// isTx returns whether the HTTP request is requesting a transaction.
func isTx(req *http.Request) (bool, error) {
	return queryParam(req, "transaction")
}

// reqParams is a convenience function to get a bunch of query params
// in one function call.
func reqParams(req *http.Request, def time.Duration) (timeout time.Duration, tx, timings, redirect bool, err error) {
	timeout, err = timeoutParam(req, def)
	if err != nil {
		return 0, false, false, false, err
	}
	tx, err = isTx(req)
	if err != nil {
		return 0, false, false, false, err
	}
	timings, err = isTimings(req)
	if err != nil {
		return 0, false, false, false, err
	}
	redirect, err = isRedirect(req)
	if err != nil {
		return 0, false, false, false, err
	}
	return timeout, tx, timings, redirect, nil
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

// timeout returns the timeout included in the query, or the given default
func timeout(req *http.Request, d time.Duration) (time.Duration, error) {
	q := req.URL.Query()
	tStr := q.Get("timeout")
	if tStr == "" {
		return d, nil
	}

	t, err := time.ParseDuration(tStr)
	if err != nil {
		return d, nil
	}
	return t, nil
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
func backupFormat(w http.ResponseWriter, r *http.Request) (store.BackupFormat, error) {
	fmt, err := fmtParam(r)
	if err != nil {
		return store.BackupBinary, err
	}
	if fmt == "sql" {
		w.Header().Set("Content-Type", "application/sql")
		return store.BackupSQL, nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	return store.BackupBinary, nil
}

func prettyEnabled(e bool) string {
	if e {
		return "enabled"
	}
	return "disabled"
}

// NormalizeAddr ensures that the given URL has a HTTP protocol prefix.
// If none is supplied, it prefixes the URL with "http://".
func NormalizeAddr(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return fmt.Sprintf("http://%s", addr)
	}
	return addr
}

// EnsureHTTPS modifies the given URL, ensuring it is using the HTTPS protocol.
func EnsureHTTPS(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return fmt.Sprintf("https://%s", addr)
	}
	return strings.Replace(addr, "http://", "https://", 1)
}

// CheckHTTPS returns true if the given URL uses HTTPS.
func CheckHTTPS(addr string) bool {
	return strings.HasPrefix(addr, "https://")
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

// queryRequestFromStrings converts a slice of strings into a command.QueryRequest
func queryRequestFromStrings(s []string, timings, tx bool) *command.QueryRequest {
	stmts := make([]*command.Statement, len(s))
	for i := range s {
		stmts[i] = &command.Statement{
			Sql: s[i],
		}

	}
	return &command.QueryRequest{
		Request: &command.Request{
			Statements:  stmts,
			Transaction: tx,
		},
		Timings: timings,
	}
}
