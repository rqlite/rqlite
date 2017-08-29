// Package http provides the HTTP server for accessing the distributed database.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"expvar"
	"fmt"
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

	sql "github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/store"
)

// Store is the interface the Raft-based database must implement.
type Store interface {
	// Execute executes a slice of queries, each of which is not expected
	// to return rows. If timings is true, then timing information will
	// be return. If tx is true, then either all queries will be executed
	// successfully or it will as though none executed.
	Execute(queries []string, timings, tx bool) ([]*sql.Result, error)

	// Query executes a slice of queries, each of which returns rows. If
	// timings is true, then timing information will be returned. If tx
	// is true, then all queries will take place while a read transaction
	// is held on the database.
	Query(queries []string, timings, tx bool, lvl store.ConsistencyLevel) ([]*sql.Rows, error)

	// Join joins the node, reachable at addr, to this node.
	Join(addr string) error

	// Remove removes the node, specified by addr, from the cluster.
	Remove(addr string) error

	// Leader returns the Raft leader of the cluster.
	Leader() string

	// Peer returns the API peer for the given address
	Peer(addr string) string

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)

	// Backup returns a byte slice representing a backup of the node state.
	Backup(leader bool) ([]byte, error)
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// Check returns whether username and password are a valid combination.
	Check(username, password string) bool

	// HasPerm returns whether username has the given perm.
	HasPerm(username string, perm string) bool
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

	CertFile string // Path to SSL certificate.
	KeyFile  string // Path to SSL private key.

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
		config, err := createTLSConfig(s.CertFile, s.KeyFile)
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
	s.logger.Println("service listening on", s.addr)

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

	if s.credentialStore != nil {
		username, password, ok := r.BasicAuth()
		if !ok || !s.credentialStore.Check(username, password) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
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
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "/remove"):
		s.handleRemove(w, r)
	case strings.HasPrefix(r.URL.Path, "/status"):
		s.handleStatus(w, r)
	case r.URL.Path == "/debug/vars" && s.Expvar:
		serveExpvar(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof") && s.Pprof:
		servePprof(w, r)
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
	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(m) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Join(remoteAddr); err != nil {
		if err == store.ErrNotLeader {
			leader := s.store.Peer(s.store.Leader())
			if leader == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leader)
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

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.store.Remove(remoteAddr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleBackup returns the consistent database snapshot.
func (s *Service) handleBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")

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

	b, err := s.store.Backup(!noLeader)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = w.Write(b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.lastBackup = time.Now()
}

// handleLoad loads the state contained in a .dump output.
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

	queries := []string{string(b)}
	results, err := s.store.Execute(queries, timings, false)
	if err != nil {
		if err == store.ErrNotLeader {
			leader := s.store.Peer(s.store.Leader())
			if leader == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leader)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
	} else {
		resp.Results = results
	}
	resp.end = time.Now()
	writeResponse(w, r, resp)
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
		"GOARCH":       runtime.GOARCH,
		"GOOS":         runtime.GOOS,
		"GOMAXPROCS":   runtime.GOMAXPROCS(0),
		"numCPU":       runtime.NumCPU(),
		"numGoroutine": runtime.NumGoroutine(),
		"version":      runtime.Version(),
	}

	httpStatus := map[string]interface{}{
		"addr":     s.Addr().String(),
		"auth":     prettyEnabled(s.credentialStore != nil),
		"redirect": s.store.Peer(s.store.Leader()),
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
		status["last_backup"] = s.lastBackup
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

	queries := []string{}
	if err := json.Unmarshal(b, &queries); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results, err := s.store.Execute(queries, timings, isTx)
	if err != nil {
		if err == store.ErrNotLeader {
			leader := s.store.Peer(s.store.Leader())
			if leader == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leader)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
	} else {
		resp.Results = results
	}
	resp.end = time.Now()
	writeResponse(w, r, resp)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	timings, err := timings(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	lvl, err := level(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the query statement(s), and do tx if necessary.
	queries := []string{}

	if r.Method == "GET" {
		query, err := stmtParam(r)
		if err != nil || query == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		queries = []string{query}
	} else {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.Body.Close()
		if err := json.Unmarshal(b, &queries); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(queries) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	results, err := s.store.Query(queries, timings, isTx, lvl)
	if err != nil {
		if err == store.ErrNotLeader {
			leader := s.store.Peer(s.store.Leader())
			if leader == "" {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			redirect := s.FormRedirect(r, leader)
			http.Redirect(w, r, redirect, http.StatusMovedPermanently)
			return
		}
		resp.Error = err.Error()
	} else {
		resp.Results = results
	}
	resp.end = time.Now()
	writeResponse(w, r, resp)
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

// FormRedirect returns the value for the "Location" header for a 301 response.
func (s *Service) FormRedirect(r *http.Request, host string) string {
	protocol := "http"
	if s.credentialStore != nil {
		protocol = "https"
	}
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
	return s.credentialStore.HasPerm(username, PermAll) || s.credentialStore.HasPerm(username, perm)
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

// serveExpvar serves registered expvar information over HTTP.
func serveExpvar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

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

// servePprof serves pprof information over HTTP.
func servePprof(w http.ResponseWriter, r *http.Request) {
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

func writeResponse(w http.ResponseWriter, r *http.Request, j *Response) {
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// createTLSConfig returns a TLS config from the given cert and key.
func createTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	var err error
	config := &tls.Config{}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
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
func level(req *http.Request) (store.ConsistencyLevel, error) {
	q := req.URL.Query()
	lvl := strings.TrimSpace(q.Get("level"))

	switch strings.ToLower(lvl) {
	case "none":
		return store.None, nil
	case "weak":
		return store.Weak, nil
	case "strong":
		return store.Strong, nil
	default:
		return store.Weak, nil
	}
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
