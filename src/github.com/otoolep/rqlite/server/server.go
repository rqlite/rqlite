package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/command"
	"github.com/otoolep/rqlite/db"

	log "code.google.com/p/log4go"
)

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

// isPretty returns whether the HTTP response body should be pretty-printed.
func isPretty(req *http.Request) (bool, error) {
	return queryParam(req, "pretty")
}

// isTransaction returns whether the client requested an explicit
// transaction for the request.
func isTransaction(req *http.Request) (bool, error) {
	return queryParam(req, "transaction")
}

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	db         *db.DB
	mutex      sync.RWMutex
}

// Creates a new server.
func New(dataDir string, dbfile string, host string, port int) *Server {
	s := &Server{
		host:   host,
		port:   port,
		path:   dataDir,
		db:     db.New(path.Join(dataDir, dbfile)),
		router: mux.NewRouter(),
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(dataDir, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(dataDir, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
}

// Returns the connection string.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

	log.Info("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.db, "")
	if err != nil {
		log.Error("Failed to create new Raft server", err.Error())
		return err
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	if leader != "" {
		// Join to leader if specified.

		log.Info("Attempting to join leader at %s", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Error("Cannot join with an existing log")
			return errors.New("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Error("Failed to join leader", err.Error())
			return err
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.Info("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err != nil {
			log.Error("Failed to join to self", err.Error())
		}

	} else {
		log.Info("Recovered from log")
	}

	log.Info("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	s.router.HandleFunc("/db", s.readHandler).Methods("GET")
	s.router.HandleFunc("/db", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	log.Info("Listening at %s", s.connectionString())

	return s.httpServer.ListenAndServe()
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	log.Debug("readHandler for URL: %s", req.URL)
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Debug("Bad HTTP request", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	r, err := s.db.Query(string(b))
	if err != nil {
		log.Debug("Bad HTTP request", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	pretty, _ := isPretty(req)
	if pretty {
		b, err = json.MarshalIndent(r, "", "    ")
	} else {
		b, err = json.Marshal(r)
	}
	if err != nil {
		log.Debug("Failed to marshal JSON data", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(b))
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	log.Debug("writeHandler for URL: %s", req.URL)
	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Debug("Bad HTTP request", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stmts := strings.Split(string(b), "\n")
	if stmts[len(stmts)-1] == "" {
		stmts = stmts[:len(stmts)-1]
	}

	// Execute the command against the Raft server.
	switch {
	case len(stmts) == 0:
		log.Debug("No database execute commands supplied")
		w.WriteHeader(http.StatusBadRequest)
		return
	case len(stmts) == 1:
		log.Debug("Single statment, implicit transaction")
		_, err = s.raftServer.Do(command.NewWriteCommand(stmts[0]))
	case len(stmts) > 1:
		log.Debug("Multistatement, transaction possible")
		transaction, _ := isTransaction(req)
		if transaction {
			log.Debug("Transaction requested")
			_, err = s.raftServer.Do(command.NewTransactionWriteCommandSet(stmts))
		} else {
			log.Debug("No transaction requested")
			// Do each individually, returning JSON respoonse
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
