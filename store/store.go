// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	sql "github.com/rqlite/rqlite/db"
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")
)

const (
	retainSnapshotCount = 2
	applyTimeout        = 10 * time.Second
	openTimeout         = 120 * time.Second
	sqliteFile          = "db.sqlite"
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
)

// QueryRequest represents a query that returns rows, and does not modify
// the database.
type QueryRequest struct {
	Queries []string
	Timings bool
	Tx      bool
	Lvl     ConsistencyLevel
}

// ExecuteRequest represents a query that returns now rows, but does modify
// the database.
type ExecuteRequest struct {
	Queries []string
	Timings bool
	Tx      bool
}

// ConsistencyLevel represents the available read consistency levels.
type ConsistencyLevel int

// Represents the available consistency levels.
const (
	None ConsistencyLevel = iota
	Weak
	Strong
)

// ClusterState defines the possible Raft states the current node can be in
type ClusterState int

// Represents the Raft cluster states
const (
	Leader ClusterState = iota
	Follower
	Candidate
	Shutdown
	Unknown
)

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	DSN    string // Any custom DSN
	Memory bool   // Whether the database is in-memory only.
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig(dsn string, memory bool) *DBConfig {
	return &DBConfig{DSN: dsn, Memory: memory}
}

// Server represents another node in the cluster.
type Server struct {
	ID   string `json:"id,omitempty"`
	Addr string `json:"addr,omitempty"`
}

// Servers is a set of Servers.
type Servers []*Server

func (s Servers) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s Servers) Len() int           { return len(s) }
func (s Servers) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Store is a SQLite database, where all changes are made via Raft consensus.
type Store struct {
	raftDir string

	mu sync.RWMutex // Sync access between queries and snapshots.

	raft     *raft.Raft // The consensus mechanism.
	raftAddr string     // The raft network address.
	raftTn   *raft.NetworkTransport
	raftID   string                // Node ID.
	logStore *raftboltdb.BoltStore // Raft log store.

	dbConf *DBConfig // SQLite database config.
	dbPath string    // Path to underlying SQLite file, if not in-memory.
	db     *sql.DB   // The underlying SQLite store.

	metaMu sync.RWMutex
	meta   map[string]map[string]string

	logger *log.Logger

	SnapshotThreshold uint64
	HeartbeatTimeout  time.Duration
	ApplyTimeout      time.Duration
}

// StoreConfig represents the configuration of the underlying Store.
type StoreConfig struct {
	DBConf  *DBConfig   // The DBConfig object for this Store.
	Dir     string      // The working directory for raft.
	Addr    string      // The network address for raft.
	AdvAddr string      // Advertised network address for Raft
	ID      string      // Node ID.
	Logger  *log.Logger // The logger to use to log stuff.
}

// New returns a new Store.
func New(c *StoreConfig) *Store {
	logger := c.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[store] ", log.LstdFlags)
	}

	if c.AdvAddr == "" {
		c.AdvAddr = c.Addr
	}

	return &Store{
		raftDir:      c.Dir,
		raftAddr:     c.Addr,
		raftID:       c.ID,
		dbConf:       c.DBConf,
		dbPath:       filepath.Join(c.Dir, sqliteFile),
		meta:         make(map[string]map[string]string),
		logger:       logger,
		ApplyTimeout: applyTimeout,
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	s.logger.Printf("opening store with node ID %s", s.raftID)

	s.logger.Printf("ensuring directory at %s exists", s.raftDir)
	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		return err
	}

	// Open underlying database.
	db, err := s.open()
	if err != nil {
		return err
	}
	s.db = db

	// Create network layer.
	tn := NewTransport()
	if err := tn.Open(s.raftAddr); err != nil {
		return err
	}
	s.raftTn = raft.NewNetworkTransport(tn, 10, 10*time.Second, nil) // XXX CONFIG NO MAGIC.

	// Is this a brand new node?
	newNode := !pathExists(filepath.Join(s.raftDir, "raft.db"))

	// Get the Raft configuration for this store.
	config := s.raftConfig()
	config.LocalID = raft.ServerID(s.raftID)
	config.Logger = log.New(os.Stderr, "[raft] ", log.LstdFlags)

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	s.logStore, err = raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, s, s.logStore, s.logStore, snapshots, s.raftTn)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	if enableSingle && newNode {
		s.logger.Printf("bootstrap needed")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: s.raftTn.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	} else {
		s.logger.Printf("no bootstrap needed")
	}

	s.raft = ra

	return nil
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
func (s *Store) Close(wait bool) error {
	if err := s.db.Close(); err != nil {
		return err
	}
	f := s.raft.Shutdown()
	if wait {
		if e := f.(raft.Future); e.Error() != nil {
			return e.Error()
		}
	}
	return s.logStore.Close()
}

// WaitForApplied waits for all Raft log entries to to be applied to the
// underlying database.
func (s *Store) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	s.logger.Printf("waiting for up to %s for application of initial logs", timeout)
	if err := s.WaitForAppliedIndex(s.raft.LastIndex(), timeout); err != nil {
		return ErrOpenTimeout
	}
	return nil
}

// IsLeader is used to determine if the current node is cluster leader
func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// State returns the current node's Raft state
func (s *Store) State() ClusterState {
	state := s.raft.State()
	switch state {
	case raft.Leader:
		return Leader
	case raft.Candidate:
		return Candidate
	case raft.Follower:
		return Follower
	case raft.Shutdown:
		return Shutdown
	default:
		return Unknown
	}
}

// Path returns the path to the store's storage directory.
func (s *Store) Path() string {
	return s.raftDir
}

// Addr returns the address of the store.
func (s *Store) Addr() string {
	return string(s.raftTn.LocalAddr())
}

// ID returns the Raft ID of the store.
func (s *Store) ID() string {
	return s.raftID
}

// Leader returns the ID of the current leader. Returns a blank
// string if there is no leader.
func (s *Store) Leader() string {
	addr := s.raft.Leader()
	if addr == "" {
		return ""
	}
	nodes, err := s.Nodes()
	if err != nil {
		return ""
	}
	for _, n := range nodes {
		if raft.ServerAddress(n.Addr) == addr {
			return n.ID
		}
	}
	return ""
}

// SetMetadata sets a key-value pair on this Store. It's scoped
// to the Raft ID of this node.
func (s *Store) SetMetadata(k, v string) error {
	// Check if state is already as desired. If so, don't
	// send a command through the Raft log.
	if func() bool {
		s.metaMu.RLock()
		defer s.metaMu.RUnlock()
		return s.metadata(k) == v
	}() {
		return nil
	}

	m := &metadataSetSub{
		NodeID: s.raftID,
		Key:    k,
		Value:  v,
	}
	c, err := newCommand(metadataSet, m)
	if err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}

	r := f.Response().(*fsmGenericResponse)
	return r.error
}

// Metadata returns the metadata value, for a given k, for this node. XXX CONSISTENCY?
func (s *Store) Metadata(k string) string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.metadata(k)
}

func (s *Store) metadata(k string) string {
	if _, ok := s.meta[s.raftID]; !ok {
		return ""
	}
	if _, ok := s.meta[s.raftID][k]; !ok {
		return ""
	}
	return s.meta[s.raftID][k]
}

// MetadataForNode returns a copy of the metadata for the given node.
func (s *Store) MetadataForNode(id string) map[string]string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	if _, ok := s.meta[s.raftID]; !ok {
		return nil
	}

	m := make(map[string]string, len(s.meta[s.raftID]))
	for k, v := range s.meta[s.raftID] {
		m[k] = v
	}
	return m
}

// Nodes returns the slice of nodes in the cluster, sorted by ID ascending.
func (s *Store) Nodes() ([]*Server, error) {
	f := s.raft.GetConfiguration()
	if f.Error() != nil {
		return nil, f.Error()
	}

	rs := f.Configuration().Servers
	servers := make([]*Server, len(rs))
	for i := range rs {
		servers[i] = &Server{
			ID:   string(rs[i].ID),
			Addr: string(rs[i].Address),
		}
	}

	sort.Sort(Servers(servers))
	return servers, nil
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *Store) WaitForLeader(timeout time.Duration) (string, error) {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			l := s.Leader()
			if l != "" {
				return l, nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

// WaitForAppliedIndex blocks until a given log index has been applied,
// or the timeout expires.
func (s *Store) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// Stats returns stats for the store.
func (s *Store) Stats() (map[string]interface{}, error) {
	fkEnabled, err := s.db.FKConstraints()
	if err != nil {
		return nil, err
	}

	dbStatus := map[string]interface{}{
		"dns":            s.dbConf.DSN,
		"fk_constraints": enabledFromBool(fkEnabled),
		"version":        sql.DBVersion,
	}
	if !s.dbConf.Memory {
		dbStatus["path"] = s.dbPath
		stat, err := os.Stat(s.dbPath)
		if err != nil {
			return nil, err
		}
		dbStatus["size"] = stat.Size()
	} else {
		dbStatus["path"] = ":memory:"
	}

	nodes, err := s.Nodes()
	if err != nil {
		return nil, err
	}

	status := map[string]interface{}{
		"node_id":            s.raftID,
		"raft":               s.raft.Stats(),
		"addr":               s.Addr(),
		"leader":             s.Leader(),
		"apply_timeout":      s.ApplyTimeout.String(),
		"heartbeat_timeout":  s.HeartbeatTimeout.String(),
		"snapshot_threshold": s.SnapshotThreshold,
		"meta":               s.meta,
		"peers":              nodes,
		"dir":                s.raftDir,
		"sqlite3":            dbStatus,
		"db_conf":            s.dbConf,
	}
	return status, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(ex *ExecuteRequest) ([]*sql.Result, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	d := &databaseSub{
		Tx:      ex.Tx,
		Queries: ex.Queries,
		Timings: ex.Timings,
	}
	c, err := newCommand(execute, d)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return nil, ErrNotLeader
		}
		return nil, e.Error()
	}

	r := f.Response().(*fsmExecuteResponse)
	return r.results, r.error
}

// Backup return a snapshot of the underlying database.
//
// If leader is true, this operation is performed with a read consistency
// level equivalent to "weak". Otherwise no guarantees are made about the
// read consistency level.
func (s *Store) Backup(leader bool) ([]byte, error) {
	if leader && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	f, err := ioutil.TempFile("", "rqlite-bak-")
	if err != nil {
		return nil, err
	}
	f.Close()
	defer os.Remove(f.Name())

	if err := s.db.Backup(f.Name()); err != nil {
		return nil, err
	}

	b, err := ioutil.ReadFile(f.Name())
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Query executes queries that return rows, and do not modify the database.
func (s *Store) Query(qr *QueryRequest) ([]*sql.Rows, error) {
	// Allow concurrent queries.
	s.mu.RLock()
	defer s.mu.RUnlock()

	if qr.Lvl == Strong {
		d := &databaseSub{
			Tx:      qr.Tx,
			Queries: qr.Queries,
			Timings: qr.Timings,
		}
		c, err := newCommand(query, d)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}

		f := s.raft.Apply(b, s.ApplyTimeout)
		if e := f.(raft.Future); e.Error() != nil {
			if e.Error() == raft.ErrNotLeader {
				return nil, ErrNotLeader
			}
			return nil, e.Error()
		}

		r := f.Response().(*fsmQueryResponse)
		return r.rows, r.error
	}

	if qr.Lvl == Weak && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	r, err := s.db.Query(qr.Queries, qr.Tx, qr.Timings)
	return r, err
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(id, addr string) error {
	s.logger.Printf("received request to join node at %s", addr)
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, the no
			// join is actually needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(id) {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", id, addr)
				return nil
			}
			if err := s.remove(id); err != nil {
				s.logger.Printf("failed to remove node: %v", err)
				return err
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		e.Error()
	}
	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

// Remove removes a node from the store, specified by ID.
func (s *Store) Remove(id string) error {
	s.logger.Printf("received request to remove node %s", id)
	if err := s.remove(id); err != nil {
		s.logger.Printf("failed to remove node %s: %s", id, err.Error())
		return err
	}
	s.logger.Printf("node %s removed successfully", id)
	return nil
}

// open opens the the in-memory or file-based database.
func (s *Store) open() (*sql.DB, error) {
	var db *sql.DB
	var err error
	if !s.dbConf.Memory {
		// as it will be rebuilt from (possibly) a snapshot and committed log entries.
		if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		db, err = sql.OpenWithDSN(s.dbPath, s.dbConf.DSN)
		if err != nil {
			return nil, err
		}
		s.logger.Println("SQLite database opened at", s.dbPath)
	} else {
		db, err = sql.OpenInMemoryWithDSN(s.dbConf.DSN)
		if err != nil {
			return nil, err
		}
		s.logger.Println("SQLite in-memory database opened")
	}
	return db, nil
}

// remove removes the node, with the given ID, from the cluster.
func (s *Store) remove(id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil {
		if f.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return f.Error()
	}
	return nil
}

// raftConfig returns a new Raft config for the store.
func (s *Store) raftConfig() *raft.Config {
	config := raft.DefaultConfig()
	if s.SnapshotThreshold != 0 {
		config.SnapshotThreshold = s.SnapshotThreshold
	}
	if s.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = s.HeartbeatTimeout
	}
	return config
}

type fsmExecuteResponse struct {
	results []*sql.Result
	error   error
}

type fsmQueryResponse struct {
	rows  []*sql.Rows
	error error
}

type fsmGenericResponse struct {
	error error
}

// Apply applies a Raft log entry to the database.
func (s *Store) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
	}

	switch c.Typ {
	case execute, query:
		var d databaseSub
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}
		if c.Typ == execute {
			r, err := s.db.Execute(d.Queries, d.Tx, d.Timings)
			return &fsmExecuteResponse{results: r, error: err}
		}
		r, err := s.db.Query(d.Queries, d.Tx, d.Timings)
		return &fsmQueryResponse{rows: r, error: err}
	case metadataSet:
		var m metadataSetSub
		if err := json.Unmarshal(c.Sub, &m); err != nil {
			return &fsmGenericResponse{error: err}
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			_, ok := s.meta[m.NodeID]
			if !ok {
				s.meta[m.NodeID] = make(map[string]string)
			}
			s.meta[m.NodeID][m.Key] = m.Value
		}()
		return &fsmGenericResponse{}
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unknown command: %v", c.Typ)}
	}
}

// Database returns a copy of the underlying database. The caller should
// ensure that no transaction is taking place during this call, or an error may
// be returned. If leader is true, this operation is performed with a read
// consistency level equivalent to "weak". Otherwise no guarantees are made
// about the read consistency level.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (s *Store) Database(leader bool) ([]byte, error) {
	if leader && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	// Ensure only one snapshot can take place at once, and block all queries.
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := ioutil.TempFile("", "rqlilte-snap-")
	if err != nil {
		return nil, err
	}
	f.Close()
	defer os.Remove(f.Name())

	if err := s.db.Backup(f.Name()); err != nil {
		return nil, err
	}

	return ioutil.ReadFile(f.Name())
}

// Snapshot returns a snapshot of the database. The caller must ensure that
// no transaction is taking place during this call. Hashicorp Raft guarantees
// that this function will not be called concurrently with Apply.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	fsm := &fsmSnapshot{}
	var err error
	fsm.database, err = s.Database(false)
	if err != nil {
		s.logger.Printf("failed to read database for snapshot: %s", err.Error())
		return nil, err
	}

	fsm.meta, err = json.Marshal(s.meta)
	if err != nil {
		s.logger.Printf("failed to encode metadata for snapshot: %s", err.Error())
		return nil, err
	}

	return fsm, nil
}

// Restore restores the node to a previous state.
func (s *Store) Restore(rc io.ReadCloser) error {
	if err := s.db.Close(); err != nil {
		return err
	}

	// Get size of database.
	var sz uint64
	if err := binary.Read(rc, binary.LittleEndian, &sz); err != nil {
		return err
	}

	// Now read in the database file data and restore.
	database := make([]byte, sz)
	if _, err := io.ReadFull(rc, database); err != nil {
		return err
	}

	var db *sql.DB
	var err error
	if !s.dbConf.Memory {
		// Write snapshot over any existing database file.
		if err := ioutil.WriteFile(s.dbPath, database, 0660); err != nil {
			return err
		}

		// Re-open it.
		db, err = sql.OpenWithDSN(s.dbPath, s.dbConf.DSN)
		if err != nil {
			return err
		}
	} else {
		// In memory. Copy to temporary file, and then load memory from file.
		f, err := ioutil.TempFile("", "rqlilte-snap-")
		if err != nil {
			return err
		}
		f.Close()
		defer os.Remove(f.Name())

		if err := ioutil.WriteFile(f.Name(), database, 0660); err != nil {
			return err
		}

		// Load an in-memory database from the snapshot now on disk.
		db, err = sql.LoadInMemoryWithDSN(f.Name(), s.dbConf.DSN)
		if err != nil {
			return err
		}
	}
	s.db = db

	// Read remaining bytes, and set to cluster meta.
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	return func() error {
		s.metaMu.Lock()
		defer s.metaMu.Unlock()
		return json.Unmarshal(b, &s.meta)
	}()
}

// RegisterObserver registers an observer of Raft events
func (s *Store) RegisterObserver(o *raft.Observer) {
	s.raft.RegisterObserver(o)
}

// DeregisterObserver deregisters an observer of Raft events
func (s *Store) DeregisterObserver(o *raft.Observer) {
	s.raft.DeregisterObserver(o)
}

type fsmSnapshot struct {
	database []byte
	meta     []byte
}

// Persist writes the snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Start by writing size of database.
		b := new(bytes.Buffer)
		sz := uint64(len(f.database))
		err := binary.Write(b, binary.LittleEndian, sz)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b.Bytes()); err != nil {
			return err
		}

		// Next write database to sink.
		if _, err := sink.Write(f.database); err != nil {
			return err
		}

		// Finally write the meta.
		if _, err := sink.Write(f.meta); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is a no-op.
func (f *fsmSnapshot) Release() {}

// enabledFromBool converts bool to "enabled" or "disabled".
func enabledFromBool(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

// pathExists returns true if the given path exists.
func pathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
