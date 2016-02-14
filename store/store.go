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
)

// Store is a SQLite database, where all changes are made via Raft consensus.
type Store struct {
	raftDir  string
	raftBind string

	mu sync.Mutex

	raft *raft.Raft // The consensus mechanism
	db   *sql.DB    // The underlying SQLite store

	logger *log.Logger
}

// New returns a new Store.
func New(dir, bind string) *Store {
	return &Store{
		raftDir:  dir,
		raftBind: bind,
		logger:   log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomesthe first node, and therefore leader, of the cluster.
func (s *Store) Open(enableSingle bool) error {
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

	// Setup the SQLite database.
	db, err := sql.Open(filepath.Join(s.raftDir, "db.sqlite"))
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

func (s *Store) Execute(stmts []string, tx bool) error {
	return nil
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

// Apply applies a Raft log entry to the database.
func (f *fsm) Apply(l *raft.Log) interface{} {
	return nil
}

// Snapshot returns a snapshot of the database.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return nil, nil
}

// Restore restores the database to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
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

func (f *fsmSnapshot) Release() {}

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
