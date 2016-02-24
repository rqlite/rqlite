// Package http provides the HTTP server for accessing the distributed database.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	sql "github.com/otoolep/rqlite/db"
)

// Store is the interface the Raft-driven database must implement.
type Store interface {
	// Execute executes a slice of queries, each of which doesn't
	// return rows. It tx is true, then all queries will be executed
	// successfully or none will be.
	Execute(queries []string, tx bool) ([]*sql.Result, error)

	// Query executes a slice of queries, each of which returns rows.
	// If tx is true, then the query will take place while a read
	// transaction is held on the database.
	Query(queries []string, tx bool) ([]*sql.Rows, error)

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error

	// Stats returns stats on the Store.
	Stats() (map[string]interface{}, error)
}

// Response represents a response from the HTTP service.
type Response struct {
	Results interface{} `json:"results,omitempty"`
	Error   string      `json:"error,omitempty"`
	Time    string      `json:"time,omitempty"`

	start time.Time
}

// NewResponse returns a new instance of response.
func NewResponse() *Response {
	return &Response{
		start: time.Now(),
	}
}

// Service provides HTTP service.
type Service struct {
	addr string
	ln   net.Listener

	store Store
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
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

	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
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
	if strings.HasPrefix(r.URL.Path, "/db") {
		if r.Method == "POST" {
			s.handleExecute(w, r)
		} else if r.Method == "GET" {
			s.handleQuery(w, r)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else if r.URL.Path == "/statistics" {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
		} else {
			s.handleStoreStats(w, r)
		}
	} else {
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

// handleStoreStats returns stats on the Raft module.
func (s *Service) handleStoreStats(w http.ResponseWriter, r *http.Request) {
	results, err := s.store.Stats()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	pretty, _ := isPretty(r)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(results, "", "    ")
	} else {
		b, err = json.Marshal(results)
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
	resp := NewResponse()

	isTx, err := isTx(r)
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

	results, err := s.store.Execute(queries, isTx)
	if err != nil {
		resp.Error = err.Error()

	} else {
		resp.Results = results
	}
	resp.Time = time.Now().Sub(resp.start).String()
	writeResponse(w, r, resp)
}

// handleQuery handles queries that do not modify the database.
func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	resp := NewResponse()

	isTx, err := isTx(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the query statement(s), and do tx if necessary.
	queries := []string{}
	query, err := stmtParam(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if query == "" {
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
	} else {
		queries = []string{query}
	}

	results, err := s.store.Execute(queries, isTx)
	if err != nil {
		resp.Error = err.Error()

	} else {
		resp.Results = results
	}
	resp.Time = time.Now().Sub(resp.start).String()
	writeResponse(w, r, resp)
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

func writeResponse(w http.ResponseWriter, r *http.Request, j *Response) {
	var b []byte
	var err error
	pretty, _ := isPretty(r)

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

// isExplain returns whether the HTTP request is requesting a explanation.
func isExplain(req *http.Request) (bool, error) {
	return queryParam(req, "explain")
}

// isLeader returns whether the HTTP request is requesting a leader check.
func isLeader(req *http.Request) (bool, error) {
	return queryParam(req, "leader")
}
