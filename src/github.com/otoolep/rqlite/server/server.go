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

type FailedSqlStmt struct {
	Sql   string `json:"sql"`
	Error string `json:"error"`
}

type StmtResponse struct {
	Time     string          `json:"time"`
	Failures []FailedSqlStmt `json:"failures"`
}

type QueryResponse struct {
	Time     string          `json:"time"`
	Failures []FailedSqlStmt `json:"failures"`
	Rows     db.RowResults   `json:"rows"`
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
	log.Trace("readHandler for URL: %s", req.URL)
	var failures = make([]FailedSqlStmt, 0)

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Trace("Bad HTTP request", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	stmt := string(b)
	startTime := time.Now()
	r, err := s.db.Query(stmt)
	if err != nil {
		log.Trace("Bad SQL statement", err.Error())
		failures = append(failures, FailedSqlStmt{stmt, err.Error()})
	}
	duration := time.Since(startTime)

	rr := QueryResponse{Time: duration.String(), Failures: failures, Rows: r}
	pretty, _ := isPretty(req)
	if pretty {
		b, err = json.MarshalIndent(rr, "", "    ")
	} else {
		b, err = json.Marshal(rr)
	}
	if err != nil {
		log.Trace("Failed to marshal JSON data", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
	} else {
		w.Write([]byte(b))
	}
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	log.Trace("writeHandler for URL: %s", req.URL)

	var failures = make([]FailedSqlStmt, 0)
	var startTime time.Time

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Trace("Bad HTTP request", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stmts := strings.Split(string(b), "\n")
	if stmts[len(stmts)-1] == "" {
		stmts = stmts[:len(stmts)-1]
	}

	log.Trace("Execute statement contains %d commands", len(stmts))
	if len(stmts) == 0 {
		log.Trace("No database execute commands supplied")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	startTime = time.Now()
	transaction, _ := isTransaction(req)
	if transaction {
		log.Trace("Transaction requested")
		_, err = s.raftServer.Do(command.NewTransactionExecuteCommandSet(stmts))
		if err != nil {
			failures = append(failures, FailedSqlStmt{stmts[0], err.Error()})
			log.Trace("Transaction failed: %s", err.Error())
		}
	} else {
		log.Trace("No transaction requested")
		for i := range stmts {
			_, err = s.raftServer.Do(command.NewExecuteCommand(stmts[i]))
			if err != nil {
				log.Trace("Execute statement %s failed: %s", stmts[i], err.Error())
				failures = append(failures, FailedSqlStmt{stmts[i], err.Error()})
			}
		}
	}
	duration := time.Since(startTime)

	wr := StmtResponse{Time: duration.String(), Failures: failures}
	pretty, _ := isPretty(req)
	if pretty {
		b, err = json.MarshalIndent(wr, "", "    ")
	} else {
		b, err = json.Marshal(wr)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
	} else {
		w.Write([]byte(b))
	}
}
