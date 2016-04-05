// Package http provides the HTTP server for accessing the distributed database.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	sql "github.com/otoolep/rqlite/db"
	"github.com/otoolep/rqlite/store"
)

// Store is the interface the Raft-driven database must implement.
type Store interface {
	// Execute executes a slice of queries, each of which doesn't
	// return rows. It tx is true, then all queries will be executed
	// successfully or none will be.
	Execute(queries []string, timings, tx bool) ([]*sql.Result, error)

	// Query executes a slice of queries, each of which returns rows.
	// If tx is true, then the query will take place while a read
	// transaction is held on the database.
	Query(queries []string, timgins, tx bool, lvl store.ConsistencyLevel) ([]*sql.Rows, error)

	// Join joins the node, reachable at addr, to this node.
	Join(addr string) error

	// Leader returns the leader of the cluster.
	Leader() string

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)

	// Backup returns the byte stream of the backup file.
	Backup(leader bool) ([]byte, error)
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

	Expvar          bool
	DisableRedirect bool // Disable leader-redirection.
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
		start: time.Now(),
	}
}

// Start starts the service.
func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			return
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.ln.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	case strings.HasPrefix(r.URL.Path, "/join"):
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "/status"):
		s.handleStatus(w, r)
	case r.URL.Path == "/debug/vars" && s.Expvar:
		serveExpvar(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleJoin handles cluster-join requests from other nodes.
func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleBackup returns the consistent database snapshot.
func (s *Service) handleBackup(w http.ResponseWriter, r *http.Request) {
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

// handleStatus returns status on the system.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	results, err := s.store.Stats()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	httpStatus := map[string]interface{}{
		"addr": s.Addr().String(),
	}

	nodeStatus := map[string]interface{}{
		"start_time": s.start,
		"uptime":     time.Since(s.start).String(),
	}

	// Build the status response.
	status := map[string]interface{}{
		"store": results,
		"http":  httpStatus,
		"node":  nodeStatus,
	}
	if !s.lastBackup.IsZero() {
		status["last_backup"] = s.lastBackup
	}

	pretty, _ := isPretty(r)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(status, "", "    ")
	} else {
		b, err = json.Marshal(status)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError) // Internal error actually
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// handleExecute handles queries that modify the database.
func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {
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
		if err == store.ErrNotLeader && !s.DisableRedirect {
			http.Redirect(w, r, s.store.Leader(), http.StatusTemporaryRedirect)
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
		if err != nil {
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
	}

	results, err := s.store.Query(queries, timings, isTx, lvl)
	if err != nil {
		if err == store.ErrNotLeader && !s.DisableRedirect {
			http.Redirect(w, r, s.store.Leader(), http.StatusTemporaryRedirect)
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

	switch lvl {
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
