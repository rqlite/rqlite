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
)

// Store is the interface the Raft-driven database must implement.
type Store interface {
	// Execute executes the set of statements, possibly within a transaction.
	Execute(stmts []string, tx bool) error

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
	if r.Method == "GET" {
		// handle SELECT
		return
	} else if r.Method == "POST" {
		// handle INSERT or UPDATE
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
	return
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
