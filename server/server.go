package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/command"
	"github.com/otoolep/rqlite/db"
	"github.com/otoolep/rqlite/interfaces"
	"github.com/otoolep/rqlite/log"
	"github.com/rcrowley/go-metrics"
)

// FailedSqlStmt contains a SQL query and an error.
type FailedSqlStmt struct {
	Sql   string `json:"sql"`
	Error string `json:"error"`
}

// StmtResponse contains a date and a list of failed
// SQL statements.
type StmtResponse struct {
	Time     string          `json:"time,omitempty"`
	Failures []FailedSqlStmt `json:"failures"`
}

// QueryResponse contains the response to a query.
type QueryResponse struct {
	Time     string          `json:"time,omitempty"`
	Failures []FailedSqlStmt `json:"failures"`
	Rows     db.RowResults   `json:"rows"`
}

// Metrics  are the server metrics user for statistics.
type Metrics struct {
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
	snapshotCreated   metrics.Counter
}

// Diagnostics contains a start time of the server.
type Diagnostics struct {
	startTime time.Time
}

// SnapshotConf contains the index when the last snapshot happened
// and a threshold for index entries since the last snapshot.
type SnapshotConf struct {
	// The index when the last snapshot happened
	lastIndex uint64

	// If the incremental number of index entries since the last
	// snapshot exceeds snapshotAfter rqlite will do a snapshot
	snapshotAfter uint64
}

// Server is is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name        string
	host        string
	port        int
	path        string
	router      *mux.Router
	raftServer  raft.Server
	httpServer  *http.Server
	dbPath      string
	db          *db.DB
	snapConf    *SnapshotConf
	metrics     *Metrics
	diagnostics *Diagnostics
	mutex       sync.Mutex
}

// ensurePrettyPrint returns a JSON representation of the object o. If
// the HTTP request requested pretty-printing, it ensures that happens.
func ensurePrettyPrint(req *http.Request, o map[string]interface{}) []byte {
	var b []byte
	pretty, _ := isPretty(req)
	if pretty {
		b, _ = json.MarshalIndent(o, "", "    ")
	} else {
		b, _ = json.Marshal(o)
	}
	return b
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

// isExplain returns whether the HTTP response body should contain metainformation
// how request processing.
func isExplain(req *http.Request) (bool, error) {
	return queryParam(req, "explain")
}

// isTransaction returns whether the client requested an explicit
// transaction for the request.
func isTransaction(req *http.Request) (bool, error) {
	return queryParam(req, "transaction")
}

// NewMetrics creates a new Metrics object.
func NewMetrics() *Metrics {
	m := &Metrics{
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
		snapshotCreated:   metrics.NewCounter(),
	}

	_ = m.registry.Register("join_success", m.joinSuccess)
	_ = m.registry.Register("join_fail", m.joinFail)
	_ = m.registry.Register("query_received", m.queryReceived)
	_ = m.registry.Register("query_success", m.querySuccess)
	_ = m.registry.Register("query_fail", m.queryFail)
	_ = m.registry.Register("execute_received", m.executeReceived)
	_ = m.registry.Register("execute_tx_received", m.executeTxReceived)
	_ = m.registry.Register("execute_success", m.executeSuccess)
	_ = m.registry.Register("execute_fail", m.executeFail)
	_ = m.registry.Register("snapshot_created", m.snapshotCreated)

	return m
}

// NewDiagnostics creates a new Diagnostics object.
func NewDiagnostics() *Diagnostics {
	d := &Diagnostics{
		startTime: time.Now(),
	}
	return d
}

// NewServer creates a new server.
func NewServer(dataDir string, dbfile string, snapAfter int, host string, port int) *Server {
	dbPath := path.Join(dataDir, dbfile)

	// Raft requires randomness.
	rand.Seed(time.Now().UnixNano())
	log.Info("Raft random seed initialized")

	// Setup commands.
	raft.RegisterCommand(&command.ExecuteCommand{})
	raft.RegisterCommand(&command.TransactionExecuteCommandSet{})
	log.Info("Raft commands registered")

	s := &Server{
		host:        host,
		port:        port,
		path:        dataDir,
		dbPath:      dbPath,
		db:          db.New(dbPath),
		snapConf:    &SnapshotConf{snapshotAfter: uint64(snapAfter)},
		metrics:     NewMetrics(),
		diagnostics: NewDiagnostics(),
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

// connectionString returns the string used to connect to this server.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

// logSnapshot logs about the snapshot that was taken.
func (s *Server) logSnapshot(err error, currentIndex, count uint64) {
	info := fmt.Sprintf("%s: snapshot of %d events at index %d", s.connectionString(), count, currentIndex)
	if err != nil {
		log.Infof("%s attempted and failed: %v", info, err)
	} else {
		log.Infof("%s completed", info)
	}
}

// ListenAndServe starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

	log.Infof("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)
	stateMachine := NewDbStateMachine(s.dbPath)
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, stateMachine, s.db, "")
	if err != nil {
		log.Errorf("Failed to create new Raft server: %s", err.Error())
		return err
	}

	log.Info("Loading latest snapshot, if any, from disk")
	if err := s.raftServer.LoadSnapshot(); err != nil && os.IsNotExist(err) {
		log.Info("no snapshot found")
	} else if err != nil {
		log.Errorf("Error loading snapshot: %s", err.Error())
	}

	transporter.Install(s.raftServer, s)
	if err := s.raftServer.Start(); err != nil {
		log.Errorf("Error starting raft server: %s", err.Error())
	}

	if leader != "" {
		// Join to leader if specified.

		log.Infof("Attempting to join leader at %s", leader)

		if !s.raftServer.IsLogEmpty() {
			log.Error("Cannot join with an existing log")
			return errors.New("Cannot join with an existing log")
		}
		if err := s.Join(leader); err != nil {
			log.Errorf("Failed to join leader: %s", err.Error())
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
			log.Errorf("Failed to join to self: %s", err.Error())
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
	s.router.HandleFunc("/raft", s.serveRaftInfo).Methods("GET")
	s.router.HandleFunc("/db", s.readHandler).Methods("GET")
	s.router.HandleFunc("/db/{tablename}", s.TableReadHandler).Methods("GET")
	s.router.HandleFunc("/db", s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	log.Infof("Listening at %s", s.connectionString())

	return s.httpServer.ListenAndServe()
}

// HandleFunc is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Join joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return nil
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Look for redirect.
	if resp.StatusCode == http.StatusTemporaryRedirect {
		leader := resp.Header.Get("Location")
		if leader == "" {
			return errors.New("Redirect requested, but no location header supplied")
		}
		u, err := url.Parse(leader)
		if err != nil {
			return errors.New("Failed to parse redirect location")
		}
		log.Infof("Redirecting to leader at %s", u.Host)
		return s.Join(u.Host)
	}

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.raftServer.State() != "leader" {
		s.leaderRedirect(w, req)
		return
	}

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
	log.Infof("readHandler for URL: %s", req.URL)
	s.metrics.queryReceived.Inc(1)

	var failures = make([]FailedSqlStmt, 0)

	// Get the query statement
	stmt, err := stmtParam(req)
	if err != nil {
		log.Tracef("Bad HTTP request: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		s.metrics.queryFail.Inc(1)
		return
	}

	startTime := time.Now()
	r, err := s.db.Query(stmt)
	if err != nil {
		log.Tracef("Bad SQL statement: %s", err.Error())
		s.metrics.queryFail.Inc(1)
		failures = append(failures, FailedSqlStmt{stmt, err.Error()})
	} else {
		s.metrics.querySuccess.Inc(1)
	}
	duration := time.Since(startTime)

	rr := QueryResponse{Failures: failures, Rows: r}
	if e, _ := isExplain(req); e {
		rr.Time = duration.String()
	}

	pretty, _ := isPretty(req)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(rr, "", "    ")
	} else {
		b, err = json.Marshal(rr)
	}
	if err != nil {
		log.Tracef("Failed to marshal JSON data: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			log.Errorf("Error writting JSON data: %s", err.Error())
		}
	}
}
func (s *Server) TableReadHandler(w http.ResponseWriter, req *http.Request) {
	log.Infof("TableReadHandler for URL: %s", req.URL)
	s.metrics.queryReceived.Inc(1)

	var failures = make([]FailedSqlStmt, 0)
	vars := mux.Vars(req)
	tablename := vars["tablename"]
	log.Infof("Getting data from table %s", tablename)
	startTime := time.Now()
	r, err := s.db.Query('Select * from '+tablename)
	if err != nil {
		log.Tracef("Bad SQL statement: %s", err.Error())
		s.metrics.queryFail.Inc(1)
		failures = append(failures, FailedSqlStmt{stmt, err.Error()})
	} else {
		s.metrics.querySuccess.Inc(1)
	}
	duration := time.Since(startTime)

	rr := QueryResponse{Failures: failures, Rows: r}
	if e, _ := isExplain(req); e {
		rr.Time = duration.String()
	}

	pretty, _ := isPretty(req)
	var b []byte
	if pretty {
		b, err = json.MarshalIndent(rr, "", "    ")
	} else {
		b, err = json.Marshal(rr)
	}
	if err != nil {
		log.Tracef("Failed to marshal JSON data: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			log.Errorf("Error writting JSON data: %s", err.Error())
		}
	}
}
func (s *Server) execute(tx bool, stmts []string) ([]FailedSqlStmt, error) {
	var failures = make([]FailedSqlStmt, 0)

	if tx {
		log.Trace("Transaction requested")
		s.metrics.executeTxReceived.Inc(1)

		_, err := s.raftServer.Do(command.NewTransactionExecuteCommandSet(stmts))
		if err != nil {
			log.Tracef("Transaction failed: %s", err.Error())
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
				log.Tracef("Execute statement %s failed: %s", stmts[i], err.Error())
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.raftServer.State() != "leader" {
		s.leaderRedirect(w, req)
		return
	}

	log.Tracef("writeHandler for URL: %s", req.URL)
	s.metrics.executeReceived.Inc(1)

	currentIndex := s.raftServer.CommitIndex()
	count := currentIndex - s.snapConf.lastIndex
	if uint64(count) > s.snapConf.snapshotAfter {
		log.Info("Committed log entries snapshot threshold reached, starting snapshot")
		err := s.raftServer.TakeSnapshot()
		s.logSnapshot(err, currentIndex, count)
		s.snapConf.lastIndex = currentIndex
		s.metrics.snapshotCreated.Inc(1)
	}

	// Read the value from the POST body.
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Tracef("Bad HTTP request: %s", err.Error())
		s.metrics.executeFail.Inc(1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stmts := strings.Split(string(b), ";")
	if stmts[len(stmts)-1] == "" {
		stmts = stmts[:len(stmts)-1]
	}

	log.Tracef("Execute statement contains %d commands", len(stmts))
	if len(stmts) == 0 {
		log.Trace("No database execute commands supplied")
		s.metrics.executeFail.Inc(1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	transaction, _ := isTransaction(req)
	startTime := time.Now()
	failures, err := s.execute(transaction, stmts)
	if err != nil {
		log.Errorf("Database mutation failed: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	duration := time.Since(startTime)

	wr := StmtResponse{Failures: failures}
	if e, _ := isExplain(req); e {
		wr.Time = duration.String()
	}

	pretty, _ := isPretty(req)
	if pretty {
		b, err = json.MarshalIndent(wr, "", "    ")
	} else {
		b, err = json.Marshal(wr)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		_, err = w.Write([]byte(b))
		if err != nil {
			log.Errorf("Error writting JSON data: %s", err.Error())
		}
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

	_, err := w.Write(ensurePrettyPrint(req, statistics))
	if err != nil {
		log.Error("failed to serve stats")
		http.Error(w, "failed to serve stats", http.StatusInternalServerError)
		return
	}
}

// serveDiagnostics returns basic server diagnostics
func (s *Server) serveDiagnostics(w http.ResponseWriter, req *http.Request) {
	diagnostics := make(map[string]interface{})
	diagnostics["started"] = s.diagnostics.startTime.String()
	diagnostics["uptime"] = time.Since(s.diagnostics.startTime).String()
	diagnostics["host"] = s.host
	diagnostics["port"] = s.port
	diagnostics["data"] = s.path
	diagnostics["database"] = s.dbPath
	diagnostics["connection"] = s.connectionString()
	diagnostics["snapafter"] = s.snapConf.snapshotAfter
	diagnostics["snapindex"] = s.snapConf.lastIndex

	_, err := w.Write(ensurePrettyPrint(req, diagnostics))
	if err != nil {
		log.Error("failed to serve diagnostics")
		http.Error(w, "failed to serve diagnostics", http.StatusInternalServerError)
		return
	}
}

// serveRaftInfo returns information about the underlying Raft server
func (s *Server) serveRaftInfo(w http.ResponseWriter, req *http.Request) {
	var peers []interface{}
	for _, v := range s.raftServer.Peers() {
		peers = append(peers, v)
	}

	info := make(map[string]interface{})
	info["name"] = s.raftServer.Name()
	info["state"] = s.raftServer.State()
	info["leader"] = s.raftServer.Leader()
	info["peers"] = peers

	_, err := w.Write(ensurePrettyPrint(req, info))
	if err != nil {
		log.Error("failed to serve raft info")
		http.Error(w, "failed to serve raft info", http.StatusInternalServerError)
		return
	}
}

// leaderRedirect returns a 307 Temporary Redirect, with the full path
// to the leader.
func (s *Server) leaderRedirect(w http.ResponseWriter, r *http.Request) {
	peers := s.raftServer.Peers()
	leader := peers[s.raftServer.Leader()]

	if leader == nil {
		// No leader available, give up.
		log.Error("attempted leader redirection, but no leader available")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("no leader available"))
		return
	}

	var u string
	for _, p := range peers {
		if p.Name == leader.Name {
			u = p.ConnectionString
			break
		}
	}
	http.Redirect(w, r, u+r.URL.Path, http.StatusTemporaryRedirect)
}
