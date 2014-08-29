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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/command"
	"github.com/otoolep/rqlite/db"
	"github.com/otoolep/rqlite/interfaces"
	"github.com/rcrowley/go-metrics"

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

type ServerMetrics struct {
	registry          metrics.Registry
	joinSuccess       metrics.Counter
	joinFail          metrics.Counter
	queryReceived     metrics.Counter
	querySuccess      metrics.Counter
	queryFail         metrics.Counter
	executeReceived   metrics.Counter
	executeTxReceived metrics.Counter
	executeSuccess    metrics.Counter
	executeFail       metrics.Counter
}

type ServerDiagnostics struct {
	startTime time.Time
}

type SnapshotConf struct {
	// The index when the last snapshot happened
	lastIndex uint64

	// If the incremental number of index entries since the last
	// snapshot exceeds snapshotAfter rqlite will do a snapshot
	snapshotAfter uint64
}

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.

type Server struct {
	name        string
	host        string
	port        int
	path        string
	router      *mux.Router
	raftServer  raft.Server
	httpServer  *http.Server
	dbFile      string
	db          *db.DB
	snapConf    *SnapshotConf
	metrics     *ServerMetrics
	diagnostics *ServerDiagnostics
	mutex       sync.Mutex
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

// Creates a new ServerMetrics object.
func NewServerMetrics() *ServerMetrics {
	m := &ServerMetrics{
		registry:          metrics.NewRegistry(),
		joinSuccess:       metrics.NewCounter(),
		joinFail:          metrics.NewCounter(),
		queryReceived:     metrics.NewCounter(),
		querySuccess:      metrics.NewCounter(),
		queryFail:         metrics.NewCounter(),
		executeReceived:   metrics.NewCounter(),
		executeTxReceived: metrics.NewCounter(),
		executeSuccess:    metrics.NewCounter(),
		executeFail:       metrics.NewCounter(),
	}

	m.registry.Register("join.succes", m.joinSuccess)
	m.registry.Register("join.fail", m.joinFail)
	m.registry.Register("query.Received", m.queryReceived)
	m.registry.Register("query.success", m.querySuccess)
	m.registry.Register("query.fail", m.queryFail)
	m.registry.Register("execute.Received", m.executeReceived)
	m.registry.Register("execute.tx.received", m.executeTxReceived)
	m.registry.Register("execute.success", m.executeSuccess)
	m.registry.Register("execute.fail", m.executeFail)
	return m
}

func NewServerDiagnostics() *ServerDiagnostics {
	d := &ServerDiagnostics{
		startTime: time.Now(),
	}
	return d
}

// Creates a new server.
func New(dataDir string, dbfile string, snapAfter int, host string, port int) *Server {
	s := &Server{
		host:        host,
		port:        port,
		path:        dataDir,
		dbFile:      dbfile,
		db:          db.New(path.Join(dataDir, dbfile)),
		metrics:     NewServerMetrics(),
		diagnostics: NewServerDiagnostics(),
		router:      mux.NewRouter(),
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

// GetStatistics returns an object storing statistics, which supports JSON
// marshalling.
func (s *Server) GetStatistics() (metrics.Registry, error) {
	return s.metrics.registry, nil
}

// Returns the connection string.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

// logSnapshot logs about the snapshot that was taken.
func (s *Server) logSnapshot(err error, currentIndex, count uint64) {
	info := fmt.Sprintf("%s: snapshot of %d events at index %d", s.connectionString, count, currentIndex)
	if err != nil {
		log.Info("%s attempted and failed: %v", info, err)
	} else {
		log.Info("%s completed", info)
	}
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

	s.router.HandleFunc("/statistics", s.serveStatistics).Methods("GET")
	s.router.HandleFunc("/diagnostics", s.serveDiagnostics).Methods("GET")
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
		s.metrics.joinFail.Inc(1)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		s.metrics.joinFail.Inc(1)
		return
	}
	s.metrics.joinSuccess.Inc(1)
}

func (s *Server) readHandler(w http.ResponseWriter, req *http.Request) {
	log.Trace("readHandler for URL: %s", req.URL)
	s.metrics.queryReceived.Inc(1)

	var failures = make([]FailedSqlStmt, 0)

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Trace("Bad HTTP request", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		s.metrics.queryFail.Inc(1)
		return
	}

	stmt := string(b)
	startTime := time.Now()
	r, err := s.db.Query(stmt)
	if err != nil {
		log.Trace("Bad SQL statement", err.Error())
		s.metrics.queryFail.Inc(1)
		failures = append(failures, FailedSqlStmt{stmt, err.Error()})
	} else {
		s.metrics.querySuccess.Inc(1)
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

func (s *Server) lockedExecute(tx bool, stmts []string) ([]FailedSqlStmt, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var failures = make([]FailedSqlStmt, 0)

	if tx {
		log.Trace("Transaction requested")
		s.metrics.executeTxReceived.Inc(1)

		_, err := s.raftServer.Do(command.NewTransactionExecuteCommandSet(stmts))
		if err != nil {
			log.Trace("Transaction failed: %s", err.Error())
			s.metrics.executeFail.Inc(1)
			failures = append(failures, FailedSqlStmt{stmts[0], err.Error()})
		} else {
			s.metrics.executeSuccess.Inc(1)
		}
	} else {
		log.Trace("No transaction requested")
		for i := range stmts {
			_, err := s.raftServer.Do(command.NewExecuteCommand(stmts[i]))
			if err != nil {
				log.Trace("Execute statement %s failed: %s", stmts[i], err.Error())
				s.metrics.executeFail.Inc(1)
				failures = append(failures, FailedSqlStmt{stmts[i], err.Error()})
			} else {
				s.metrics.executeSuccess.Inc(1)
			}

		}
	}

	return failures, nil
}

func (s *Server) writeHandler(w http.ResponseWriter, req *http.Request) {
	log.Trace("writeHandler for URL: %s", req.URL)
	s.metrics.executeReceived.Inc(1)

	currentIndex := s.raftServer.CommitIndex()
	count := currentIndex - s.snapConf.lastIndex
	if uint64(count) > s.snapConf.snapshotAfter {
		err := s.raftServer.TakeSnapshot()
		s.logSnapshot(err, currentIndex, count)
		s.snapConf.lastIndex = currentIndex
	}

	var startTime time.Time

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Trace("Bad HTTP request", err.Error())
		s.metrics.executeFail.Inc(1)
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
		s.metrics.executeFail.Inc(1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	transaction, _ := isTransaction(req)
	startTime = time.Now()
	failures, err := s.lockedExecute(transaction, stmts)
	if err != nil {
		log.Error("Database mutation failed: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.Write([]byte(b))
	}
}

// serveStatistics returns the statistics for the program
func (s *Server) serveStatistics(w http.ResponseWriter, req *http.Request) {
	statistics := make(map[string]interface{})
	resources := map[string]interfaces.Statistics{"server": s}
	for k, v := range resources {
		s, err := v.GetStatistics()
		if err != nil {
			log.Error("failed to get " + k + " stats")
			http.Error(w, "failed to get "+k+" stats", http.StatusInternalServerError)
			return
		}
		statistics[k] = s
	}

	var b []byte
	var err error
	pretty, _ := isPretty(req)
	if pretty {
		b, err = json.MarshalIndent(statistics, "", "    ")
	} else {
		b, err = json.Marshal(statistics)
	}
	if err != nil {
		log.Error("failed to JSON marshal statistics map")
		http.Error(w, "failed to JSON marshal statistics map", http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

// serveDiagnostics returns basic server diagnostics
func (s *Server) serveDiagnostics(w http.ResponseWriter, req *http.Request) {
	diagnostics := make(map[string]string)
	diagnostics["started"] = s.diagnostics.startTime.String()
	diagnostics["uptime"] = time.Since(s.diagnostics.startTime).String()
	diagnostics["data"] = s.path
	diagnostics["database"] = s.dbFile
	diagnostics["connection"] = s.connectionString()
	diagnostics["snapafter"] = strconv.FormatUint(s.snapConf.snapshotAfter, 10)
	diagnostics["snapindex"] = strconv.FormatUint(s.snapConf.lastIndex, 10)
	var b []byte
	pretty, _ := isPretty(req)
	if pretty {
		b, _ = json.MarshalIndent(diagnostics, "", "    ")
	} else {
		b, _ = json.Marshal(diagnostics)
	}
	w.Write(b)
}
