// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	sql "github.com/otoolep/rqlite/db"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	sqliteFile          = "db.sqlite"
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
)

var (
	// ErrFieldsRequired is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")
)

type ConsistencyLevel int

const (
	None ConsistencyLevel = iota
	Weak
	Strong
)

type commandType int

const (
	execute commandType = iota
	query
)

type command struct {
	Typ     commandType `json:"typ,omitempty"`
	Tx      bool        `json:"tx,omitempty"`
	Queries []string    `json:"queries,omitempty"`
	Timings bool        `json:"timings,omitempty"`
}

// Store is a SQLite database, where all changes are made via Raft consensus.
type Store struct {
	raftDir  string
	raftBind string

	mu sync.RWMutex // Sync access between queries and snapshots.

	ln     *networkLayer // Raft network between nodes.
	raft   *raft.Raft    // The consensus mechanism.
	dbConf *sql.Config   // SQLite database config.
	dbPath string        // Path to database file.
	db     *sql.DB       // The underlying SQLite store.

	logger *log.Logger
}

// New returns a new Store.
func New(dbConf *sql.Config, dir, bind string) *Store {
	dbPath := filepath.Join(dir, sqliteFile)
	if dbConf.Memory {
		dbPath = ":memory:"
	}

	return &Store{
		raftDir:  dir,
		raftBind: bind,
		dbConf:   dbConf,
		dbPath:   dbPath,
		logger:   log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		return err
	}

	// Create the database. Unless it's a memory-based database, tt must be deleted
	if !s.dbConf.Memory {
		// as it will be rebuilt from (possibly) a snapshot and committed log entries.
		if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	db, err := sql.OpenWithConfiguration(s.dbPath, s.dbConf)
	if err != nil {
		return err
	}
	s.db = db
	s.logger.Println("SQLite database opened at", s.dbConf.FQDSN(s.dbPath))

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.raftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Setup Raft communication.
	ln, err := net.Listen("tcp", s.raftBind)
	if err != nil {
		return err
	}
	s.ln = newnetworkLayer(ln)
	transport := raft.NewNetworkTransport(s.ln, 3, 10*time.Second, os.Stderr)

	// Create peer storage.
	peerStore := raft.NewJSONPeers(s.raftDir, transport)

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, s, logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	return nil
}

// Close closes the store.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return err
	}
	f := s.raft.Shutdown()
	if e := f.(raft.Future); e.Error() != nil {
		return e.Error()
	}
	return nil
}

// Path returns the path to the store's storage directory.
func (s *Store) Path() string {
	return s.raftDir
}

func (s *Store) Addr() net.Addr {
	return s.ln.Addr()
}

// Leader returns the current leader. Returns a blank string if there is
// no leader.
func (s *Store) Leader() string {
	return s.raft.Leader()
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
	dbStatus := map[string]interface{}{
		"path":    s.dbPath,
		"dns":     s.dbConf.FQDSN(s.dbPath),
		"version": sql.DBVersion,
	}
	if !s.dbConf.Memory {
		stat, err := os.Stat(s.dbPath)
		if err != nil {
			return nil, err
		}
		dbStatus["size"] = stat.Size()
	}

	status := map[string]interface{}{
		"raft":    s.raft.Stats(),
		"addr":    s.Addr().String(),
		"leader":  s.Leader(),
		"sqlite3": dbStatus,
	}
	return status, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(queries []string, timings, tx bool) ([]*sql.Result, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	c := &command{
		Typ:     execute,
		Tx:      tx,
		Queries: queries,
		Timings: timings,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}

	r := f.Response().(*fsmExecuteResponse)
	return r.results, r.error
}

// Backup return a consistent snapshot of the underlying database.
func (s *Store) Backup(leader bool) ([]byte, error) {
	if leader && s.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader")
	}

	f, err := ioutil.TempFile("", "rqlilte-bak-")
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
func (s *Store) Query(queries []string, timings, tx bool, lvl ConsistencyLevel) ([]*sql.Rows, error) {
	// Allow concurrent queries.
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lvl == Strong {
		c := &command{
			Typ:     query,
			Tx:      tx,
			Queries: queries,
			Timings: timings,
		}
		b, err := json.Marshal(c)
		if err != nil {
			return nil, err
		}

		f := s.raft.Apply(b, raftTimeout)
		if e := f.(raft.Future); e.Error() != nil {
			return nil, e.Error()
		}

		r := f.Response().(*fsmQueryResponse)
		return r.rows, r.error
	}

	if lvl == Weak && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	r, err := s.db.Query(queries, tx, timings)
	return r, err
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) error {
	s.logger.Printf("received request to join node at %s", addr)

	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

type fsmExecuteResponse struct {
	results []*sql.Result
	error   error
}

type fsmQueryResponse struct {
	rows  []*sql.Rows
	error error
}

// Apply applies a Raft log entry to the database.
func (s *Store) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	if c.Typ == execute {
		r, err := s.db.Execute(c.Queries, c.Tx, c.Timings)
		return &fsmExecuteResponse{results: r, error: err}
	}
	r, err := s.db.Query(c.Queries, c.Tx, c.Timings)
	return &fsmQueryResponse{rows: r, error: err}
}

// Snapshot returns a snapshot of the database. The caller must ensure that
// no transaction is taking place during this call. Hashsicorp Raft guarantees
// that this function will not be called concurrently with Apply.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
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

	b, err := ioutil.ReadFile(f.Name())
	if err != nil {
		log.Printf("Failed to generate snapshot: %s", err.Error())
		return nil, err
	}
	return &fsmSnapshot{data: b}, nil
}

// Restore restores the database to a previous state.
func (s *Store) Restore(rc io.ReadCloser) error {
	if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(s.dbPath, b, 0660); err != nil {
		return err
	}

	db, err := sql.OpenWithConfiguration(s.dbPath, s.dbConf)
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

type fsmSnapshot struct {
	data []byte
}

// Persist writes the snapshot to the give sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Write data to sink.
		if _, err := sink.Write(f.data); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is a no-op.
func (f *fsmSnapshot) Release() {}

// readPeersJSON reads the peers from the path.
func readPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}
