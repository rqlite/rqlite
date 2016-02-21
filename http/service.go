// Package http provides the HTTP server for accessing the distributed database.
// It also provides the endpoint for other nodes to join an existing cluster.
package http

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
)

// Store is the interface the Raft-driven database must implement.
type Store interface {
	// Execute executes a slice of queries, each of which doesn't
	// return rows. It tx is true, then all queries will be executed
	// successfully or none will be.
	Execute(queries []string, tx bool) ([]sql.Result, error)

	// Query executes a slice of queries, each of which returns rows.
	// If tx is true, then the query will take place while a read
	// transaction is held on the database.
	Query(queries []string, tx bool) ([]*sql.Rows, error)

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error
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
	if strings.HasPrefix(r.URL.Path, "/query") {
		s.handleQuery(w, r)
	} else if r.URL.Path == "/join" {
		s.handleJoin(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

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

func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	// Get the query statement
	query, err := stmtParam(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rows, err := s.store.Query([]string{query}, false)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	pretty, _ := isPretty(r)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(*rows, "", "    ")
	} else {
		b, err = json.Marshal(*rows)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
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
	if stmt == "" {
		return "", fmt.Errorf(`required parameter 'q' is missing`)
	}
	return stmt, nil
}

// isPretty returns whether the HTTP response body should be pretty-printed.
func isPretty(req *http.Request) (bool, error) {
	return queryParam(req, "pretty")
}
