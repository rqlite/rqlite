// Package http provides the HTTP server for accessing the distributed database.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"bytes"
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
	sql "github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/store"
)

// Database is the interface any queryable system must implement
type Database interface {
	// Execute executes a slice of queries, each of which is not expected
	// to return rows. If timings is true, then timing information will
	// be return. If tx is true, then either all queries will be executed
	// successfully or it will as though none executed.
	Execute(er *command.ExecuteRequest) ([]*sql.Result, error)

	// ExecuteOrAbort performs the same function as Execute(), but ensures
	// any transactions are aborted in case of any error.
	ExecuteOrAbort(er *command.ExecuteRequest) ([]*sql.Result, error)

	// Query executes a slice of queries, each of which returns rows. If
	// timings is true, then timing information will be returned. If tx
	// is true, then all queries will take place while a read transaction
	// is held on the database.
	Query(qr *command.QueryRequest) ([]*sql.Rows, error)
}

// Store is the interface the Raft-based database must implement.
type Store interface {
	Database

	// Join joins the node with the given ID, reachable at addr, to this node.
	Join(id, addr string, voter bool, metadata map[string]string) error

	// Remove removes the node, specified by id, from the cluster.
	Remove(id string) error

	// Metadata returns the value for the given node ID, for the given key.
	Metadata(id, key string) string

	// Leader returns the Raft address of the leader of the cluster.
	LeaderID() (string, error)

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)

	// Backup wites backup of the node state to dst
	Backup(leader bool, f store.BackupFormat, dst io.Writer) error
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
	Stats() (interface{}, error)
}

// Response represents a response from the HTTP service.
type Response struct {
	Results interface{} `json:"results,omitempty"`
	Error   string      `json:"error,omitempty"`
	Time    float64     `json:"time,omitempty"`

	start time.Time
	end   time.Time
}

// stats captures stats for the HTTP service.
var stats *expvar.Map

const (
	numExecutions = "executions"
	numQueries    = "queries"
	numBackups    = "backups"
	numLoad       = "loads"
	numJoins      = "joins"

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
)

func init() {
	stats = expvar.NewMap("http")
	stats.Add(numExecutions, 0)
	stats.Add(numQueries, 0)
	stats.Add(numBackups, 0)
	stats.Add(numLoad, 0)
	stats.Add(numJoins, 0)
}

// SetTime sets the Time attribute of the response. This way it will be present
// in the serialized JSON version.
func (r *Response) SetTime() {
	r.Time = r.end.Sub(r.start).Seconds()
}

// NewResponse returns a new instance of response.
func NewResponse() *Response {
	return &Response{
		start: time.Now(),
	}
}

// Service provides HTTP service.
type Service struct {
	addr string       // Bind address of the HTTP service.
	ln   net.Listener // Service listener

	store Store // The Raft-backed database store.

	start      time.Time // Start up time.
	lastBackup time.Time // Time of last successful backup.

	statusMu sync.RWMutex
	statuses map[string]Statuser

	CACertFile string // Path to root X.509 certificate.
	CertFile   string // Path to SSL certificate.
	KeyFile    string // Path to SSL private key.

	credentialStore CredentialStore

	Expvar bool
	Pprof  bool

	BuildInfo map[string]interface{}

	logger *log.Logger
}

// New returns an uninitialized HTTP service. If credentials is nil, then
// the service performs no authentication and authorization checks.
func New(addr string, store Store, credentials CredentialStore) *Service {
	return &Service{
		addr:            addr,
		store:           store,
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
		config, err := createTLSConfig(s.CertFile, s.KeyFile, s.CACertFile)
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
	var m map[string]string
	if _, ok := md["meta"].(map[string]interface{}); ok {
		m = make(map[string]string)
		for k, v := range md["meta"].(map[string]interface{}) {
			m[k] = v.(string)
		}
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

	if err := s.store.Join(remoteID.(string), remoteAddr.(string), voter.(bool), m); err != nil {
		if err == store.ErrNotLeader {
			leaderAPIAddr := s.LeaderAPIAddr()
			leaderProto := s.LeaderAPIProto()
			if leaderAPIAddr == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leaderProto, leaderAPIAddr)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}

		b := bytes.NewBufferString(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(b.Bytes())
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
			leaderProto := s.LeaderAPIProto()
			if leaderAPIAddr == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leaderProto, leaderAPIAddr)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}

		w.WriteHeader(http.StatusInternalServerError)
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
			leaderProto := s.LeaderAPIProto()
			if leaderAPIAddr == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leaderProto, leaderAPIAddr)
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

	timings, err := timings(r)
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

	results, err := s.store.ExecuteOrAbort(er)
	if err != nil {
		if err == store.ErrNotLeader {
			leaderAPIAddr := s.LeaderAPIAddr()
			leaderProto := s.LeaderAPIProto()
			if leaderAPIAddr == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leaderProto, leaderAPIAddr)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
	} else {
		resp.Results = results
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

	results, err := s.store.Stats()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
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
		"addr":     s.Addr().String(),
		"auth":     prettyEnabled(s.credentialStore != nil),
		"redirect": s.LeaderAPIAddr(),
	}

	nodeStatus := map[string]interface{}{
		"start_time": s.start,
		"uptime":     time.Since(s.start).String(),
	}

	// Build the status response.
	status := map[string]interface{}{
		"runtime": rt,
		"store":   results,
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
			w.WriteHeader(http.StatusInternalServerError)
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

	isTx, err := isTx(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	timings, err := timings(r)
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

	results, err := s.store.Execute(er)
	if err != nil {
		if err == store.ErrNotLeader {
			leaderAPIAddr := s.LeaderAPIAddr()
			leaderProto := s.LeaderAPIProto()
			if leaderAPIAddr == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leaderProto, leaderAPIAddr)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
	} else {
		resp.Results = results
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

	isTx, err := isTx(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	timings, err := timings(r)
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

	results, err := s.store.Query(qr)
	if err != nil {
		if err == store.ErrNotLeader {
			leaderAPIAddr := s.LeaderAPIAddr()
			leaderProto := s.LeaderAPIProto()
			if leaderAPIAddr == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leaderProto, leaderAPIAddr)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
	} else {
		resp.Results = results
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
func (s *Service) FormRedirect(r *http.Request, protocol, host string) string {
	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}
	return fmt.Sprintf("%s://%s%s%s", protocol, host, r.URL.Path, rq)
}

// CheckRequestPerm returns true if authentication is enabled and the user contained
// in the BasicAuth request has either PermAll, or the given perm.
func (s *Service) CheckRequestPerm(r *http.Request, perm string) bool {
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
	id, err := s.store.LeaderID()
	if err != nil {
		return ""
	}
	return s.store.Metadata(id, "api_addr")
}

// LeaderAPIProto returns the protocol used by the leader, as known by this node.
func (s *Service) LeaderAPIProto() string {
	id, err := s.store.LeaderID()
	if err != nil {
		return "http"
	}

	p := s.store.Metadata(id, "api_proto")
	if p == "" {
		return "http"
	}
	return p
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
	timings, _ := timings(r)

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
func createTLSConfig(certFile, keyFile, caCertFile string) (*tls.Config, error) {
	var err error
	config := &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
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

// queryParam returns whether the given query param is set to true.
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

// isTx returns whether the HTTP request is requesting a transaction.
func isTx(req *http.Request) (bool, error) {
	return queryParam(req, "transaction")
}

// noLeader returns whether processing should skip the leader check.
func noLeader(req *http.Request) (bool, error) {
	return queryParam(req, "noleader")
}

// timings returns whether timings are requested.
func timings(req *http.Request) (bool, error) {
	return queryParam(req, "timings")
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
