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
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	sql "github.com/rqlite/rqlite/db"
)

var (
	// ErrFieldsRequired is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	sqliteFile          = "db.sqlite"
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
)

// Transport is the interface the network service must provide.
type Transport interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// commandType are commands that affect the state of the cluster, and must go through Raft.
type commandType int

const (
	execute commandType = iota // Commands which modify the database.
	query                      // Commands which query the database.
	peer                       // Commands that modify peers map.
)

type command struct {
	Typ commandType     `json:"typ,omitempty"`
	Sub json.RawMessage `json:"sub,omitempty"`
}

func newCommand(t commandType, d interface{}) (*command, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &command{
		Typ: t,
		Sub: b,
	}, nil

}

// databaseSub is a command sub which involves interaction with the database.
type databaseSub struct {
	Tx      bool     `json:"tx,omitempty"`
	Queries []string `json:"queries,omitempty"`
	Timings bool     `json:"timings,omitempty"`
}

// peersSub is a command which sets the API address for a Raft address.
type peersSub map[string]string

// ConsistencyLevel represents the available read consistency levels.
type ConsistencyLevel int

const (
	None ConsistencyLevel = iota
	Weak
	Strong
)

// clusterMeta represents cluster meta which must be kept in consensus.
type clusterMeta struct {
	APIPeers map[string]string // Map from Raft address to API address
}

// newClusterMeta returns an initialized cluster meta store.
func newClusterMeta() *clusterMeta {
	return &clusterMeta{
		APIPeers: make(map[string]string),
	}
}

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	DSN    string // Any custom DSN
	Memory bool   // Whether the database is in-memory only.
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig(dsn string, memory bool) *DBConfig {
	return &DBConfig{DSN: dsn, Memory: memory}
}

// Store is a SQLite database, where all changes are made via Raft consensus.
type Store struct {
	raftDir string

	mu sync.RWMutex // Sync access between queries and snapshots.

	raft          *raft.Raft // The consensus mechanism.
	raftTransport Transport
	dbConf        *DBConfig // SQLite database config.
	dbPath        string    // Path to underlying SQLite file, if not in-memory.
	db            *sql.DB   // The underlying SQLite store.
	joinRequired  bool      // Whether an explicit join is required.

	metaMu sync.RWMutex
	meta   *clusterMeta

	logger *log.Logger
}

// New returns a new Store.
func New(dbConf *DBConfig, dir string, tn Transport) *Store {
	return &Store{
		raftDir:       dir,
		raftTransport: tn,
		dbConf:        dbConf,
		dbPath:        filepath.Join(dir, sqliteFile),
		meta:          newClusterMeta(),
		logger:        log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		return err
	}

	// Create the database. Unless it's a memory-based database, it must be deleted
	var db *sql.DB
	var err error
	if !s.dbConf.Memory {
		// as it will be rebuilt from (possibly) a snapshot and committed log entries.
		if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		db, err = sql.OpenWithDSN(s.dbPath, s.dbConf.DSN)
		if err != nil {
			return err
		}
		s.logger.Println("SQLite database opened at", s.dbPath)
	} else {
		db, err = sql.OpenInMemoryWithDSN(s.dbConf.DSN)
		if err != nil {
			return err
		}
		s.logger.Println("SQLite in-memory database opened")
	}
	s.db = db

	// Setup Raft configuration.
	config := raft.DefaultConfig()

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(s.raftDir, "peers.json"))
	if err != nil {
		return err
	}
	s.joinRequired = len(peers) <= 1

	// Allow the node to entry single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if enableSingle && len(peers) <= 1 {
		s.logger.Println("enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// Setup Raft communication.
	transport := raft.NewNetworkTransport(s.raftTransport, 3, 10*time.Second, os.Stderr)

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
	return nil
}

// JoinRequired returns whether the node needs to join a cluster after being opened.
func (s *Store) JoinRequired() bool {
	return s.joinRequired
}

// Path returns the path to the store's storage directory.
func (s *Store) Path() string {
	return s.raftDir
}

func (s *Store) Addr() net.Addr {
	return s.raftTransport.Addr()
}

// Leader returns the current leader. Returns a blank string if there is
// no leader.
func (s *Store) Leader() string {
	return s.raft.Leader()
}

// Peer returns the API address for the given addr. If there is no peer
// for the address, it returns the empty string.
func (s *Store) Peer(addr string) string {
	return s.meta.APIPeers[addr]
}

// APIPeers return the map of Raft addresses to API addresses.
func (s *Store) APIPeers() (map[string]string, error) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	peers := make(map[string]string, len(s.meta.APIPeers))
	for k, v := range s.meta.APIPeers {
		peers[k] = v
	}
	return peers, nil
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
		"dns":     s.dbConf.DSN,
		"version": sql.DBVersion,
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

	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	status := map[string]interface{}{
		"raft":    s.raft.Stats(),
		"addr":    s.Addr().String(),
		"leader":  s.Leader(),
		"meta":    s.meta,
		"sqlite3": dbStatus,
	}
	return status, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(queries []string, timings, tx bool) ([]*sql.Result, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	d := &databaseSub{
		Tx:      tx,
		Queries: queries,
		Timings: timings,
	}
	c, err := newCommand(execute, d)
	if err != nil {
		return nil, err
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
		d := &databaseSub{
			Tx:      tx,
			Queries: queries,
			Timings: timings,
		}
		c, err := newCommand(query, d)
		if err != nil {
			return nil, err
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

// UpdateAPIPeers updates the cluster-wide peer information.
func (s *Store) UpdateAPIPeers(peers map[string]string) error {
	c, err := newCommand(peer, peers)
	if err != nil {
		return err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		return e.Error()
	}
	return nil
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
	case peer:
		var d peersSub
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			for k, v := range d {
				s.meta.APIPeers[k] = v
			}
		}()
		return &fsmGenericResponse{}
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unknown command: %v", c.Typ)}
	}
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

	fsm := &fsmSnapshot{}
	fsm.database, err = ioutil.ReadFile(f.Name())
	if err != nil {
		log.Printf("Failed to read database for snapshot: %s", err.Error())
		return nil, err
	}

	fsm.meta, err = json.Marshal(s.meta)
	if err != nil {
		log.Printf("Failed to encode meta for snapshot: %s", err.Error())
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
