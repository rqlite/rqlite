// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"encoding/json"
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
)

type command struct {
	Tx      bool     `json:"tx,omitempty"`
	Queries []string `json:"queries,omitempty"`
}

// Store is a SQLite database, where all changes are made via Raft consensus.
type Store struct {
	raftDir  string
	raftBind string

	mu sync.RWMutex // Sync access between queries and snapshots.

	raft   *raft.Raft // The consensus mechanism
	dbConf *sql.Config
	dbPath string
	db     *sql.DB // The underlying SQLite store

	logger *log.Logger

	SQLiteDB string
}

// New returns a new Store.
func New(dbConf *sql.Config, dir, bind string) *Store {
	return &Store{
		raftDir:  dir,
		raftBind: bind,
		dbConf:   dbConf,
		dbPath:   filepath.Join(dir, sqliteFile),
		logger:   log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	// Create the database. It must be deleted as it will be rebuilt from
	// (possibly) a snapshot and committed log entries.
	if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	db, err := sql.OpenWithConfiguration(s.dbPath, s.dbConf)
	if err != nil {
		return err
	}
	s.db = db

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
	addr, err := net.ResolveTCPAddr("tcp", s.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create peer storage.
	peerStore := raft.NewJSONPeers(s.raftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	return nil
}

// Close closes the store.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Stats() (map[string]interface{}, error) {
	return map[string]interface{}{"raft": s.raft.Stats()}, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(queries []string, tx bool) ([]*sql.Result, error) {
	if s.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader")
	}

	c := &command{
		Tx:      tx,
		Queries: queries,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, raftTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return nil, e.Error()
	}

	r := f.Response().(*fsmResponse)
	return r.results, r.error
}

// Query executes queries that return rows, and do not modify the database.
func (s *Store) Query(queries []string, tx bool) ([]*sql.Rows, error) {
	// Allow concurrent queries.
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, err := s.db.Query(queries, tx, true)
	return r, err
}

// Join joins a node, located at addr, to this store. The node must be ready to
// respond to Raft communications at that address.
func (s *Store) Join(addr string) error {
	s.logger.Printf("received join request for remote node as %s", addr)

	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Printf("node at %s joined successfully", addr)
	return nil
}

type fsm Store

type fsmResponse struct {
	results []*sql.Result
	error   error
}

// Apply applies a Raft log entry to the database.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	r, err := f.db.Execute(c.Queries, c.Tx, true)

	return &fsmResponse{results: r, error: err}
}

// Snapshot returns a snapshot of the database. The caller must ensure that
// no transaction is taking place during this call. Hashsicorp Raft guarantees
// that this function will not be called concurrently with Apply.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// Ensure only one snapshot can take place at once, and block all queries.
	f.mu.Lock()
	defer f.mu.Unlock()

	b, err := ioutil.ReadFile(f.dbPath)
	if err != nil {
		log.Printf("Failed to generate snapshot: %s", err.Error())
		return nil, err
	}
	return &fsmSnapshot{data: b}, nil
}

// Restore restores the database to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	if err := os.Remove(f.dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(f.dbPath, b, 0660); err != nil {
		return err
	}

	db, err := sql.OpenWithConfiguration(f.dbPath, f.dbConf)
	if err != nil {
		return err
	}
	f.db = db

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
