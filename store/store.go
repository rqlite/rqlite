// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	sdb "github.com/rqlite/rqlite/db"
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrInvalidBackupFormat is returned when the requested backup format
	// is not valid.
	ErrInvalidBackupFormat = errors.New("invalid backup format")

	// ErrTransactionActive is returned when an operation is blocked by an
	// active transaction.
	ErrTransactionActive = errors.New("transaction in progress")

	// errDefaultConnection is returned when an attempt is made to delete the
	// default connection.
	errDefaultConnection = errors.New("cannot delete default connection")
)

const (
	retainSnapshotCount = 2
	applyTimeout        = 10 * time.Second
	openTimeout         = 120 * time.Second
	sqliteFile          = "db.sqlite"
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
	connectionPoolCount = 5
	connectionTimeout   = 10 * time.Second
	raftLogCacheSize    = 512
)

const (
	numSnaphots        = "num_snapshots"
	numSnaphotsBlocked = "num_snapshots_blocked"
	numBackups         = "num_backups"
	numRestores        = "num_restores"
)

const defaultConnID = 0

// ExecerQueryer is generic connection for interacting with a database.
type ExecerQueryer interface {
	// Execute executes queries that return no rows, but do modify the database.
	Execute(ex *ExecuteRequest) (*ExecuteResponse, error)

	// ExecuteOrAbort executes the requests, but aborts any active transaction
	// on the underlying database in the case of any error.
	ExecuteOrAbort(ex *ExecuteRequest) (resp *ExecuteResponse, retErr error)

	// Query executes queries that return rows, and do not modify the database.
	Query(qr *QueryRequest) (*QueryResponse, error)
}

// ExecerQueryerCloser is generic connection for interacting with a database,
// which also allows release of its underlying resources. Once closed, it
// cannot be reused.
type ExecerQueryerCloser interface {
	ExecerQueryer
	io.Closer
}

// ExecerQueryerCloserIDer is generic connection that also returns an ID that
// can be used to identify the object.
type ExecerQueryerCloserIDer interface {
	ExecerQueryerCloser
	ID() uint64
}

// BackupFormat represents the backup formats supported by the Store.
type BackupFormat int

const (
	// BackupSQL is dump of the database in SQL text format.
	BackupSQL BackupFormat = iota

	// BackupBinary is a copy of the SQLite database file.
	BackupBinary
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("store")
	stats.Add(numSnaphots, 0)
	stats.Add(numBackups, 0)
	stats.Add(numRestores, 0)
}

// RaftResponse is the Raft metadata that will be included with responses, if
// the associated request modified the Raft log.
type RaftResponse struct {
	Index  uint64 `json:"index,omitempty"`
	NodeID string `json:"node_id,omitempty"`
}

// QueryRequest represents a query that returns rows, and does not modify
// the database.
type QueryRequest struct {
	Queries []string
	Timings bool
	Tx      bool
	Lvl     ConsistencyLevel
}

// QueryResponse encapsulates a response to a query.
type QueryResponse struct {
	Rows []*sdb.Rows
	Time float64
	Raft *RaftResponse
}

// ExecuteRequest represents a query that returns now rows, but does modify
// the database.
type ExecuteRequest struct {
	Queries []string
	Timings bool
	Tx      bool
}

// ExecuteResponse encapsulates a response to an execute.
type ExecuteResponse struct {
	Results []*sdb.Result
	Time    float64
	Raft    RaftResponse
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

// Store is a SQLite database, where all changes are made via Raft consensus.
type Store struct {
	raftDir string

	raft   *raft.Raft // The consensus mechanism.
	ln     Listener
	raftTn *raft.NetworkTransport
	raftID string    // Node ID.
	dbConf *DBConfig // SQLite database config.
	dbPath string    // Path to underlying SQLite file, if not in-memory.

	connsMu sync.RWMutex
	randSrc *rand.Rand
	db      *sdb.DB                // The underlying SQLite database.
	conns   map[uint64]*Connection // Database connections under management.
	dbConn  *sdb.Conn              // Hidden connection to underlying SQLite database.

	raftLog    raft.LogStore         // Persistent log store.
	raftStable raft.StableStore      // Persistent k-v store.
	boltStore  *raftboltdb.BoltStore // Physical store.

	metaMu sync.RWMutex
	meta   map[string]map[string]string

	restoreMu sync.RWMutex // Restore needs exclusive access to database.

	logger *log.Logger

	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	HeartbeatTimeout  time.Duration
	ApplyTimeout      time.Duration
}

// StoreConfig represents the configuration of the underlying Store.
type StoreConfig struct {
	DBConf *DBConfig   // The DBConfig object for this Store.
	Dir    string      // The working directory for raft.
	Tn     Transport   // The underlying Transport for raft.
	ID     string      // Node ID.
	Logger *log.Logger // The logger to use to log stuff.
}

// New returns a new Store.
func New(ln Listener, c *StoreConfig) *Store {
	logger := c.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[store] ", log.LstdFlags)
	}

	return &Store{
		ln:           ln,
		raftDir:      c.Dir,
		raftID:       c.ID,
		dbConf:       c.DBConf,
		dbPath:       filepath.Join(c.Dir, sqliteFile),
		randSrc:      rand.New(rand.NewSource(time.Now().UnixNano())),
		conns:        make(map[uint64]*Connection),
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

	// Create underlying database.
	if err := s.createDatabase(); err != nil {
		return err
	}

	// Get utility connection to database.
	conn, err := s.db.Connect()
	if err != nil {
		return err
	}
	s.dbConn = conn
	s.conns[defaultConnID] = NewConnection(s.dbConn, s, defaultConnID)

	// Is this a brand new node?
	newNode := !pathExists(filepath.Join(s.raftDir, "raft.db"))

	// Create Raft-compatible network layer.
	s.raftTn = raft.NewNetworkTransport(NewTransport(s.ln),
		connectionPoolCount, connectionTimeout, nil)

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
	s.boltStore, err = raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	s.raftStable = s.boltStore
	s.raftLog, err = raft.NewLogCache(raftLogCacheSize, s.boltStore)
	if err != nil {
		return fmt.Errorf("new cached store: %s", err)
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, s, s.raftLog, s.raftStable, snapshots, s.raftTn)
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

// Connect returns a new connection to the database. Changes made to the database
// through this connection are applied via the Raft consensus system. The Store
// must have been opened first. Must be called on the leader or an error will
// we returned.
//
// Any connection returned by this call are READ_COMMITTED isolated from all
// other connections, including the connection built-in to the Store itself.
func (s *Store) Connect() (ExecerQueryerCloserIDer, error) {
	// Randomly-selected connection ID must be part of command so
	// that all nodes use the same value as connection ID.
	connID := func() uint64 {
		s.connsMu.Lock()
		defer s.connsMu.Unlock()
		for {
			// Make sure we get an unused ID.
			id := s.randSrc.Uint64()
			if _, ok := s.conns[id]; !ok {
				s.conns[id] = nil
				return id
			}
		}
	}()

	d := &connectionSub{connID}
	cmd, err := newCommand(connect, d)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(cmd)
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

	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	return s.conns[connID], nil
}

// Execute executes queries that return no rows, but do modify the database.
// Changes made to the database through this call are applied via the Raft
// consensus system. The Store must have been opened first. Must be called
// on the leader or an error will we returned. The changes are made using
// the database connection built-in to the Store.
func (s *Store) Execute(ex *ExecuteRequest) (*ExecuteResponse, error) {
	return s.execute(nil, ex)
}

// ExecuteOrAbort executes the requests, but aborts any active transaction
// on the underlying database in the case of any error. Any changes are made
// using the database connection built-in to the Store.
func (s *Store) ExecuteOrAbort(ex *ExecuteRequest) (resp *ExecuteResponse, retErr error) {
	return s.executeOrAbort(nil, ex)
}

// Query executes queries that return rows, and do not modify the database.
// The queries are made using the database connection built-in to the Store.
// Depending on the read consistency requested, it may or may not need to be
// called on the leader.
func (s *Store) Query(qr *QueryRequest) (*QueryResponse, error) {
	return s.query(nil, qr)
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
func (s *Store) Close(wait bool) error {
	// XXX CLOSE OTHER CONNECTIONS
	if err := s.dbConn.Close(); err != nil {
		return err
	}
	s.dbConn = nil
	s.db = nil

	if s.raft != nil {
		f := s.raft.Shutdown()
		if wait {
			if e := f.(raft.Future); e.Error() != nil {
				return e.Error()
			}
		}
		s.raft = nil
	}

	if s.boltStore != nil {
		if err := s.boltStore.Close(); err != nil {
			return err
		}
		s.boltStore = nil
	}
	s.raftLog = nil
	s.raftStable = nil
	return nil
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

// Connection returns the connection for the given ID.
func (s *Store) Connection(id uint64) (*Connection, bool) {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	c, ok := s.conns[id]
	return c, ok
}

// LeaderAddr returns the Raft address of the current leader. Returns a
// blank string if there is no leader.
func (s *Store) LeaderAddr() string {
	return string(s.raft.Leader())
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	addr := s.LeaderAddr()
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return "", err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}
	return "", nil
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
			l := s.LeaderAddr()
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
	fkEnabled, err := s.dbConn.FKConstraints()
	if err != nil {
		return nil, err
	}

	dbStatus := map[string]interface{}{
		"dsn":            s.dbConf.DSN,
		"fk_constraints": enabledFromBool(fkEnabled),
		"version":        sdb.DBVersion,
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
	leaderID, err := s.LeaderID()
	if err != nil {
		return nil, err
	}

	status := map[string]interface{}{
		"node_id": s.raftID,
		"raft":    s.raft.Stats(),
		"addr":    s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    s.LeaderAddr(),
		},
		"apply_timeout":      s.ApplyTimeout.String(),
		"heartbeat_timeout":  s.HeartbeatTimeout.String(),
		"snapshot_threshold": s.SnapshotThreshold,
		"metadata":           s.meta,
		"nodes":              nodes,
		"dir":                s.raftDir,
		"sqlite3":            dbStatus,
		"db_conf":            s.dbConf,
	}
	return status, nil
}

// Backup writes a snapshot of the underlying database to dst
//
// If leader is true, this operation is performed with a read consistency
// level equivalent to "weak". Otherwise no guarantees are made about the
// read consistency level.
func (s *Store) Backup(leader bool, fmt BackupFormat, dst io.Writer) error {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()

	if leader && s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	var err error
	if fmt == BackupBinary {
		err = s.database(leader, dst)
		if err != nil {
			return err
		}
	} else if fmt == BackupSQL {
		if err := s.dbConn.Dump(dst); err != nil {
			return err
		}
	} else {
		return ErrInvalidBackupFormat
	}
	stats.Add(numBackups, 1)
	return nil
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(id, addr string, metadata map[string]string) error {
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
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request",
					id, addr)
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

	if err := s.setMetadata(id, metadata); err != nil {
		return err
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

// Metadata returns the value for a given key, for a given node ID.
func (s *Store) Metadata(id, key string) string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	if _, ok := s.meta[id]; !ok {
		return ""
	}
	v, ok := s.meta[id][key]
	if ok {
		return v
	}
	return ""
}

// SetMetadata adds the metadata md to any existing metadata for
// this node.
func (s *Store) SetMetadata(md map[string]string) error {
	return s.setMetadata(s.raftID, md)
}

// setMetadata adds the metadata md to any existing metadata for
// the given node ID.
func (s *Store) setMetadata(id string, md map[string]string) error {
	// Check local data first.
	if func() bool {
		s.metaMu.RLock()
		defer s.metaMu.RUnlock()
		if _, ok := s.meta[id]; ok {
			for k, v := range md {
				if s.meta[id][k] != v {
					return false
				}
			}
			return true
		}
		return false
	}() {
		// Local data is same as data being pushed in,
		// nothing to do.
		return nil
	}

	c, err := newMetadataSetCommand(id, md)
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
		e.Error()
	}

	return f.Response().(*fsmGenericResponse).error
}

// disconnect removes a connection to the database, a connection
// which was previously established via Raft consensus.
func (s *Store) disconnect(c *Connection) error {
	d := &connectionSub{c.ConnID}
	cmd, err := newCommand(disconnect, d)
	if err != nil {
		return err
	}
	b, err := json.Marshal(cmd)
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
	return f.Response().(*fsmGenericResponse).error
}

// Execute executes queries that return no rows, but do modify the database. If connection
// is nil then the utility connection is used.
func (s *Store) execute(c *Connection, ex *ExecuteRequest) (*ExecuteResponse, error) {
	if c == nil {
		s.connsMu.RLock()
		c = s.conns[defaultConnID]
		s.connsMu.RUnlock()
	}
	c.SetLastUsedNow()

	start := time.Now()

	d := &databaseSub{
		ConnID:  c.ConnID,
		Tx:      ex.Tx,
		Queries: ex.Queries,
		Timings: ex.Timings,
	}
	cmd, err := newCommand(execute, d)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(cmd)
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
	return &ExecuteResponse{
		Results: r.results,
		Time:    time.Since(start).Seconds(),
		Raft:    RaftResponse{f.Index(), s.raftID},
	}, r.error
}

func (s *Store) executeOrAbort(c *Connection, ex *ExecuteRequest) (resp *ExecuteResponse, retErr error) {
	if c == nil {
		s.connsMu.RLock()
		c = s.conns[defaultConnID]
		s.connsMu.RUnlock()
	}

	defer func() {
		var errored bool
		for i := range resp.Results {
			if resp.Results[i].Error != "" {
				errored = true
				break
			}
		}
		if retErr != nil || errored {
			if err := c.AbortTransaction(); err != nil {
				c.logger.Printf("WARNING: failed to abort transaction on %s: %s",
					c, err.Error())
			}
		}
	}()
	return c.store.execute(c, ex)
}

// Query executes queries that return rows, and do not modify the database. If
// connection is nil, then the utility connection is used.
func (s *Store) query(c *Connection, qr *QueryRequest) (*QueryResponse, error) {
	if c == nil {
		s.connsMu.RLock()
		c = s.conns[defaultConnID]
		s.connsMu.RUnlock()
	}
	c.SetLastUsedNow()

	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()
	start := time.Now()

	if qr.Lvl == Strong {
		d := &databaseSub{
			ConnID:  c.ConnID,
			Tx:      qr.Tx,
			Queries: qr.Queries,
			Timings: qr.Timings,
		}
		cmd, err := newCommand(query, d)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(cmd)
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
		return &QueryResponse{
			Rows: r.rows,
			Time: time.Since(start).Seconds(),
			Raft: &RaftResponse{f.Index(), s.raftID},
		}, err
	}

	if qr.Lvl == Weak && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	r, err := c.db.Query(qr.Queries, qr.Tx, qr.Timings)
	return &QueryResponse{
		Rows: r,
		Time: time.Since(start).Seconds(),
	}, err
}

// createDatabase creates the the in-memory or file-based database.
func (s *Store) createDatabase() error {
	var db *sdb.DB
	var err error
	if !s.dbConf.Memory {
		// as it will be rebuilt from (possibly) a snapshot and committed log entries.
		if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		db, err = sdb.New(s.dbPath, s.dbConf.DSN, false)
		if err != nil {
			return err
		}
		s.logger.Println("SQLite database opened at", s.dbPath)
	} else {
		db, err = sdb.New(s.dbPath, s.dbConf.DSN, true)
		if err != nil {
			return err
		}
		s.logger.Println("SQLite in-memory database opened")
	}
	s.db = db
	return nil
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

	c, err := newCommand(metadataDelete, id)
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	f = s.raft.Apply(b, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		e.Error()
	}

	return nil
}

// raftConfig returns a new Raft config for the store.
func (s *Store) raftConfig() *raft.Config {
	config := raft.DefaultConfig()
	if s.SnapshotThreshold != 0 {
		config.SnapshotThreshold = s.SnapshotThreshold
	}
	if s.SnapshotInterval != 0 {
		config.SnapshotInterval = s.SnapshotInterval
	}
	if s.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = s.HeartbeatTimeout
	}
	return config
}

type fsmExecuteResponse struct {
	results []*sdb.Result
	error   error
}

type fsmQueryResponse struct {
	rows  []*sdb.Rows
	error error
}

type fsmGenericResponse struct {
	error error
}

// Apply applies a Raft log entry to the database.
func (s *Store) Apply(l *raft.Log) interface{} {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()

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

		s.connsMu.RLock()
		conn, ok := s.conns[d.ConnID]
		s.connsMu.RUnlock()
		if !ok {
			return &fsmGenericResponse{error: fmt.Errorf("connection %d does not exist", d.ConnID)}
		}

		if c.Typ == execute {
			txChange := NewTxStateChange(conn)
			r, err := conn.db.Execute(d.Queries, d.Tx, d.Timings)
			txChange.CheckAndSet()
			return &fsmExecuteResponse{results: r, error: err}
		}
		r, err := conn.db.Query(d.Queries, d.Tx, d.Timings)
		return &fsmQueryResponse{rows: r, error: err}
	case metadataSet:
		var d metadataSetSub
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			if _, ok := s.meta[d.RaftID]; !ok {
				s.meta[d.RaftID] = make(map[string]string)
			}
			for k, v := range d.Data {
				s.meta[d.RaftID][k] = v
			}
		}()
		return &fsmGenericResponse{}
	case metadataDelete:
		var d string
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			delete(s.meta, d)
		}()
		return &fsmGenericResponse{}
	case connect:
		var d connectionSub
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}

		conn, err := s.db.Connect()
		if err != nil {
			return &fsmGenericResponse{error: err}
		}
		s.connsMu.Lock()
		s.conns[d.ConnID] = NewConnection(conn, s, d.ConnID)
		s.connsMu.Unlock()
		return d.ConnID
	case disconnect:
		var d connectionSub
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}

		if d.ConnID == defaultConnID {
			return &fsmGenericResponse{error: errDefaultConnection}
		}

		s.connsMu.Lock()
		conn := s.conns[d.ConnID]
		if err := conn.db.Close(); err != nil {
			return &fsmGenericResponse{error: err}
		}
		delete(s.conns, d.ConnID)
		s.connsMu.Unlock()

		return &fsmGenericResponse{}
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unknown command: %v", c.Typ)}
	}
}

// Database copies contents of the underlying SQLite file to dst
func (s *Store) database(leader bool, dst io.Writer) error {
	if leader && s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f, err := ioutil.TempFile("", "rqlilte-snap-")
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	os.Remove(f.Name())
	db, err := sdb.New(f.Name(), "", false)
	if err != nil {
		return err
	}
	conn, err := db.Connect()
	if err != nil {
		return err
	}

	if err := s.dbConn.Backup(conn); err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}

	of, err := os.Open(f.Name())
	if err != nil {
		return err
	}
	defer of.Close()

	_, err = io.Copy(dst, of)
	return err
}

// Snapshot returns a snapshot of the store. The caller must ensure that
// no Raft transaction is taking place during this call. Hashicorp Raft
// guarantees that this function will not be called concurrently with Apply.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.restoreMu.RLock()
	defer s.restoreMu.RUnlock()

	// Snapshots are not permitted while any connection has a transaction
	// in progress, because it's not possible (not without a lot of extra
	// code anyway) to capture the state of a connection during a transaction
	// on that connection. Since only during Apply() can a connection change
	// its transaction state, and Apply() is never called concurrently with
	// this call, it's safe to check transaction state here across all connections.
	if err := func() error {
		s.connsMu.Lock()
		defer s.connsMu.Unlock()
		for _, c := range s.conns {
			if c.TransactionActive() {
				stats.Add(numSnaphotsBlocked, 1)
				return ErrTransactionActive
			}
		}
		return nil
	}(); err != nil {
		return nil, err
	}

	// Copy the database.
	fsm := &fsmSnapshot{}
	var buf bytes.Buffer
	var err error
	err = s.database(false, &buf)
	if err != nil {
		s.logger.Printf("failed to read database for snapshot: %s", err.Error())
		return nil, err
	}
	fsm.database = buf.Bytes()

	// Copy the node metadata.
	fsm.meta, err = json.Marshal(s.meta)
	if err != nil {
		s.logger.Printf("failed to encode meta for snapshot: %s", err.Error())
		return nil, err
	}

	// Copy the active connections.
	fsm.connections, err = json.Marshal(s.conns)
	if err != nil {
		s.logger.Printf("failed to encode connections for snapshot: %s", err.Error())
		return nil, err
	}
	stats.Add(numSnaphots, 1)

	return fsm, nil
}

// Restore restores the node to a previous state.
func (s *Store) Restore(rc io.ReadCloser) error {
	s.restoreMu.Lock()
	defer s.restoreMu.Unlock()

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

	// Create temp file and write incoming database to there.
	temp, err := tempfile()
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()

	if _, err := temp.Write(database); err != nil {
		return err
	}

	// Create new database from file, connect, and load
	// existing database from that.
	db, err := sdb.New(temp.Name(), "", false)
	if err != nil {
		return err
	}
	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := s.dbConn.Load(conn); err != nil {
		return err
	}

	// Get size of meta, read those bytes, and set to meta.
	if err := binary.Read(rc, binary.LittleEndian, &sz); err != nil {
		return err
	}
	meta := make([]byte, sz)
	if _, err := io.ReadFull(rc, meta); err != nil {
		return err
	}
	err = func() error {
		s.metaMu.Lock()
		defer s.metaMu.Unlock()
		return json.Unmarshal(meta, &s.meta)
	}()
	if err != nil {
		return err
	}

	// Get size of connections, read those bytes, and set to connections.
	if err := binary.Read(rc, binary.LittleEndian, &sz); err != nil {
		return err
	}
	conns := make([]byte, sz)
	if _, err := io.ReadFull(rc, conns); err != nil {
		return err
	}
	err = func() error {
		s.connsMu.Lock()
		defer s.connsMu.Unlock()
		if err := json.Unmarshal(conns, &s.conns); err != nil {
			return err
		}
		for _, c := range s.conns {
			dbConn, err := s.db.Connect()
			if err != nil {
				return err
			}
			c.Restore(dbConn, s)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	stats.Add(numRestores, 1)
	return nil
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
	database    []byte
	meta        []byte
	connections []byte
}

// Persist writes the snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	// sizeWriter writes the size of given byte slize to the
	// raft sink, followed by the byte slice itself.
	sizeWriter := func(s raft.SnapshotSink, b []byte) error {
		buf := new(bytes.Buffer)
		sz := uint64(len(b))
		err := binary.Write(buf, binary.LittleEndian, sz)
		if err != nil {
			return err
		}
		if _, err := s.Write(buf.Bytes()); err != nil {
			return err
		}
		if _, err := s.Write(b); err != nil {
			return err
		}
		return nil
	}

	if err := func() error {
		for _, b := range [][]byte{f.database, f.meta, f.connections} {
			if err := sizeWriter(sink, b); err != nil {
				return err
			}
		}
		// Close the sink.
		return sink.Close()
	}(); err != nil {
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

// tempfile returns a temporary file for use
func tempfile() (*os.File, error) {
	f, err := ioutil.TempFile("", "rqlilte-snap-")
	if err != nil {
		return nil, err
	}
	return f, nil
}
