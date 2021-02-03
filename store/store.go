// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/command"
	legacy "github.com/rqlite/rqlite/command/legacy"
	sql "github.com/rqlite/rqlite/db"
	rlog "github.com/rqlite/rqlite/log"
)

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrStaleRead is returned if the executing the query would violate the
	// requested freshness.
	ErrStaleRead = errors.New("stale read")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrInvalidBackupFormat is returned when the requested backup format
	// is not valid.
	ErrInvalidBackupFormat = errors.New("invalid backup format")
)

const (
	raftDBPath          = "raft.db" // Changing this will break backwards compatibility.
	retainSnapshotCount = 2
	applyTimeout        = 10 * time.Second
	openTimeout         = 120 * time.Second
	sqliteFile          = "db.sqlite"
	leaderWaitDelay     = 100 * time.Millisecond
	appliedWaitDelay    = 100 * time.Millisecond
	connectionPoolCount = 5
	connectionTimeout   = 10 * time.Second
	raftLogCacheSize    = 512
	trailingScale       = 1.25
)

const (
	numSnaphots             = "num_snapshots"
	numBackups              = "num_backups"
	numRestores             = "num_restores"
	numUncompressedCommands = "num_uncompressed_commands"
	numCompressedCommands   = "num_compressed_commands"
	numLegacyCommands       = "num_legacy_commands"
)

// BackupFormat represents the format of database backup.
type BackupFormat int

const (
	// BackupSQL is the plaintext SQL command format.
	BackupSQL BackupFormat = iota

	// BackupBinary is a SQLite file backup format.
	BackupBinary
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("store")
	stats.Add(numSnaphots, 0)
	stats.Add(numBackups, 0)
	stats.Add(numRestores, 0)
	stats.Add(numUncompressedCommands, 0)
	stats.Add(numCompressedCommands, 0)
	stats.Add(numLegacyCommands, 0)
}

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
	db     *sql.DB   // The underlying SQLite store.

	reqMarshaller *command.RequestMarshaler // Request marshaler for writing to log.
	raftLog       raft.LogStore             // Persistent log store.
	raftStable    raft.StableStore          // Persistent k-v store.
	boltStore     *rlog.Log                 // Physical store.

	onDiskCreated        bool      // On disk database actually created?
	snapsExistOnOpen     bool      // Any snaps present when store opens?
	firstIdxOnOpen       uint64    // First index on log when Store opens.
	lastIdxOnOpen        uint64    // Last index on log when Store opens.
	lastCommandIdxOnOpen uint64    // Last command index on log when Store opens.
	firstLogAppliedT     time.Time // Time first log is applied
	appliedOnOpen        uint64    // Number of logs applied at open.
	openT                time.Time // Timestamp when Store opens.

	txMu    sync.RWMutex // Sync between snapshots and query-level transactions.
	queryMu sync.RWMutex // Sync queries generally with other operations.

	metaMu sync.RWMutex
	meta   map[string]map[string]string

	logger *log.Logger

	ShutdownOnRemove   bool
	SnapshotThreshold  uint64
	SnapshotInterval   time.Duration
	LeaderLeaseTimeout time.Duration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	ApplyTimeout       time.Duration
	RaftLogLevel       string

	numTrailingLogs uint64
}

// IsNewNode returns whether a node using raftDir would be a brand new node.
// It also means that the window this node joining a different cluster has passed.
func IsNewNode(raftDir string) bool {
	// If there is any pre-existing Raft state, then this node
	// has already been created.
	return !pathExists(filepath.Join(raftDir, raftDBPath))
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
		ln:            ln,
		raftDir:       c.Dir,
		raftID:        c.ID,
		dbConf:        c.DBConf,
		dbPath:        filepath.Join(c.Dir, sqliteFile),
		reqMarshaller: command.NewRequestMarshaler(),
		meta:          make(map[string]map[string]string),
		logger:        logger,
		ApplyTimeout:  applyTimeout,
	}
}

// Open opens the Store. If enableBootstrap is set, then this node becomes a
// standalone node. If not set, then the calling layer must know that this
// node has pre-existing state, or the calling layer will trigger a join
// operation after opening the Store.
func (s *Store) Open(enableBootstrap bool) error {
	s.openT = time.Now()
	s.logger.Printf("opening store with node ID %s", s.raftID)

	s.logger.Printf("ensuring directory at %s exists", s.raftDir)
	err := os.MkdirAll(s.raftDir, 0755)
	if err != nil {
		return err
	}

	// Create Raft-compatible network layer.
	s.raftTn = raft.NewNetworkTransport(NewTransport(s.ln), connectionPoolCount, connectionTimeout, nil)

	// Don't allow control over trailing logs directly, just implement a policy.
	s.numTrailingLogs = uint64(float64(s.SnapshotThreshold) * trailingScale)

	config := s.raftConfig()
	config.LocalID = raft.ServerID(s.raftID)

	// Create the snapshot store. This allows Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	snaps, err := snapshots.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %s", err)
	}
	s.logger.Printf("%d preexisting snapshots present", len(snaps))
	s.snapsExistOnOpen = len(snaps) > 0

	// Create the log store and stable store.
	s.boltStore, err = rlog.NewLog(filepath.Join(s.raftDir, raftDBPath))
	if err != nil {
		return fmt.Errorf("new log store: %s", err)
	}
	s.raftStable = s.boltStore
	s.raftLog, err = raft.NewLogCache(raftLogCacheSize, s.boltStore)
	if err != nil {
		return fmt.Errorf("new cached store: %s", err)
	}

	// Get some info about the log, before any more entries are committed.
	if err := s.setLogInfo(); err != nil {
		return fmt.Errorf("set log info: %s", err)
	}
	s.logger.Printf("first log index: %d, last log index: %d, last command log index: %d:",
		s.firstIdxOnOpen, s.lastIdxOnOpen, s.lastCommandIdxOnOpen)

	// If an on-disk database has been requested, and there are no snapshots, and
	// there are no commands in the log, then this is the only opportunity to
	// create that on-disk database file before Raft initializes.
	if !s.dbConf.Memory && !s.snapsExistOnOpen && s.lastCommandIdxOnOpen == 0 {
		s.db, err = s.openOnDisk(nil)
		if err != nil {
			return fmt.Errorf("failed to open on-disk database")
		}
		s.onDiskCreated = true
	} else {
		// We need an in-memory database, at least for bootstrapping purposes.
		s.db, err = s.openInMemory(nil)
		if err != nil {
			return fmt.Errorf("failed to open in-memory database")
		}
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, s, s.raftLog, s.raftStable, snapshots, s.raftTn)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	if enableBootstrap {
		s.logger.Printf("executing new cluster bootstrap")
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
		s.logger.Printf("no cluster bootstrap requested")
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

// LeaderAddr returns the address of the current leader. Returns a
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

// SetRequestCompression allows low-level control over the compression threshold
// for the request marshaler.
func (s *Store) SetRequestCompression(batch, size int) {
	s.reqMarshaller.BatchThreshold = batch
	s.reqMarshaller.SizeThreshold = size
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

	dbSz, err := s.db.Size()
	if err != nil {
		return nil, err
	}
	dbStatus := map[string]interface{}{
		"dsn":            s.dbConf.DSN,
		"fk_constraints": enabledFromBool(fkEnabled),
		"version":        sql.DBVersion,
		"db_size":        dbSz,
	}
	if s.dbConf.Memory {
		dbStatus["path"] = ":memory:"
	} else {
		dbStatus["path"] = s.dbPath
		if s.onDiskCreated {
			if dbStatus["size"], err = s.db.FileSize(); err != nil {
				return nil, err
			}
		}
	}

	nodes, err := s.Nodes()
	if err != nil {
		return nil, err
	}
	leaderID, err := s.LeaderID()
	if err != nil {
		return nil, err
	}

	raftStats := s.raft.Stats()
	ls, err := s.logSize()
	if err != nil {
		return nil, err
	}
	raftStats["log_size"] = strconv.FormatInt(ls, 10)

	status := map[string]interface{}{
		"node_id": s.raftID,
		"raft":    raftStats,
		"addr":    s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    s.LeaderAddr(),
		},
		"apply_timeout":      s.ApplyTimeout.String(),
		"heartbeat_timeout":  s.HeartbeatTimeout.String(),
		"election_timeout":   s.ElectionTimeout.String(),
		"snapshot_threshold": s.SnapshotThreshold,
		"snapshot_interval":  s.SnapshotInterval,
		"trailing_logs":      s.numTrailingLogs,
		"request_marshaler":  s.reqMarshaller.Stats(),
		"metadata":           s.meta,
		"nodes":              nodes,
		"dir":                s.raftDir,
		"sqlite3":            dbStatus,
		"db_conf":            s.dbConf,
	}
	return status, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(ex *command.ExecuteRequest) ([]*sql.Result, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	return s.execute(ex)
}

// ExecuteOrAbort executes the requests, but aborts any active transaction
// on the underlying database in the case of any error.
func (s *Store) ExecuteOrAbort(ex *command.ExecuteRequest) (results []*sql.Result, retErr error) {
	defer func() {
		var errored bool
		if results != nil {
			for i := range results {
				if results[i].Error != "" {
					errored = true
					break
				}
			}
		}
		if retErr != nil || errored {
			if err := s.db.AbortTransaction(); err != nil {
				s.logger.Printf("WARNING: failed to abort transaction: %s", err.Error())
			}
		}
	}()
	return s.execute(ex)
}

func (s *Store) execute(ex *command.ExecuteRequest) ([]*sql.Result, error) {
	b, compressed, err := s.reqMarshaller.Marshal(ex)
	if err != nil {
		return nil, err
	}
	if compressed {
		stats.Add(numCompressedCommands, 1)
	} else {
		stats.Add(numUncompressedCommands, 1)
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_EXECUTE,
		SubCommand: b,
		Compressed: compressed,
	}

	b, err = command.Marshal(c)
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

// Backup writes a snapshot of the underlying database to dst
//
// If leader is true, this operation is performed with a read consistency
// level equivalent to "weak". Otherwise no guarantees are made about the
// read consistency level.
func (s *Store) Backup(leader bool, fmt BackupFormat, dst io.Writer) error {
	if leader && s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	if fmt == BackupBinary {
		if err := s.database(leader, dst); err != nil {
			return err
		}
	} else if fmt == BackupSQL {
		if err := s.db.Dump(dst); err != nil {
			return err
		}
	} else {
		return ErrInvalidBackupFormat
	}

	stats.Add(numBackups, 1)
	return nil
}

// Query executes queries that return rows, and do not modify the database.
func (s *Store) Query(qr *command.QueryRequest) ([]*sql.Rows, error) {
	s.queryMu.RLock()
	defer s.queryMu.RUnlock()

	if qr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG {
		b, compressed, err := s.reqMarshaller.Marshal(qr)
		if err != nil {
			return nil, err
		}
		if compressed {
			stats.Add(numCompressedCommands, 1)
		} else {
			stats.Add(numUncompressedCommands, 1)
		}

		c := &command.Command{
			Type:       command.Command_COMMAND_TYPE_QUERY,
			SubCommand: b,
			Compressed: compressed,
		}

		b, err = command.Marshal(c)
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

	if qr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	if qr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_NONE && qr.Freshness > 0 &&
		time.Since(s.raft.LastContact()).Nanoseconds() > qr.Freshness {
		return nil, ErrStaleRead
	}

	// Read straight from database. If a transaction is requested, we must block
	// certain other database operations.
	if qr.Request.Transaction {
		s.txMu.Lock()
		defer s.txMu.Unlock()
	}
	return s.db.Query(qr.Request, qr.Timings)
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(id, addr string, voter bool, metadata map[string]string) error {
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

	var f raft.IndexFuture
	if voter {
		f = s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	} else {

		f = s.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	}
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}

	if err := s.setMetadata(id, metadata); err != nil {
		return err
	}

	s.logger.Printf("node at %s joined successfully as %s", addr, prettyVoter(voter))
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

	ms := &command.MetadataSet{
		RaftId: id,
		Data:   md,
	}
	bms, err := command.MarshalMetadataSet(ms)
	if err != nil {
		return err
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_METADATA_SET,
		SubCommand: bms,
	}
	bc, err := command.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(bc, s.ApplyTimeout)
	if e := f.(raft.Future); e.Error() != nil {
		if e.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return e.Error()
	}

	return nil
}

// openInMemory returns an in-memory database. If b is non-nil, then the
// database will be initialized with the contents of b.
func (s *Store) openInMemory(b []byte) (db *sql.DB, err error) {
	if b == nil {
		db, err = sql.OpenInMemoryWithDSN(s.dbConf.DSN)
	} else {
		db, err = sql.DeserializeInMemoryWithDSN(b, s.dbConf.DSN)
	}
	return
}

// openOnDisk opens an on-disk database file at the Store's configured path. If
// b is non-nil, any preexisting file will first be overwritten with those contents.
// Otherwise any pre-existing file will be removed before the database is opened.
func (s *Store) openOnDisk(b []byte) (*sql.DB, error) {
	if err := os.Remove(s.dbPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if b != nil {
		if err := ioutil.WriteFile(s.dbPath, b, 0660); err != nil {
			return nil, err
		}
	}
	return sql.OpenWithDSN(s.dbPath, s.dbConf.DSN)
}

// setLogInfo records some key indexs about the log.
func (s *Store) setLogInfo() error {
	var err error
	s.firstIdxOnOpen, err = s.boltStore.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get last index: %s", err)
	}
	s.lastIdxOnOpen, err = s.boltStore.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to get last index: %s", err)
	}
	s.lastCommandIdxOnOpen, err = s.boltStore.LastCommandIndex()
	if err != nil {
		return fmt.Errorf("failed to get last command index: %s", err)
	}
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

	md := command.MetadataDelete{
		RaftId: id,
	}
	bmd, err := command.MarshalMetadataDelete(&md)
	if err != nil {
		return err
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_METADATA_DELETE,
		SubCommand: bmd,
	}
	bc, err := command.Marshal(c)
	if err != nil {
		return err
	}

	f = s.raft.Apply(bc, s.ApplyTimeout)
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
	config.ShutdownOnRemove = s.ShutdownOnRemove
	config.LogLevel = s.RaftLogLevel
	if s.SnapshotThreshold != 0 {
		config.SnapshotThreshold = s.SnapshotThreshold
		config.TrailingLogs = s.numTrailingLogs
	}
	if s.SnapshotInterval != 0 {
		config.SnapshotInterval = s.SnapshotInterval
	}
	if s.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = s.LeaderLeaseTimeout
	}
	if s.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = s.HeartbeatTimeout
	}
	if s.ElectionTimeout != 0 {
		config.ElectionTimeout = s.ElectionTimeout
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
func (s *Store) Apply(l *raft.Log) (e interface{}) {
	defer func() {
		if l.Index <= s.lastCommandIdxOnOpen {
			// In here means at least one command entry was in the log when the Store
			// opened.
			s.appliedOnOpen++
			if l.Index == s.lastCommandIdxOnOpen {
				s.logger.Printf("%d committed log entries applied in %s, took %s since open",
					s.appliedOnOpen, time.Since(s.firstLogAppliedT), time.Since(s.openT))

				// Last command log applied. Time to switch to on-disk database?
				if s.dbConf.Memory {
					s.logger.Println("continuing use of in-memory database")
				} else {
					// Since we're here, it means that a) an on-disk database was requested
					// *and* there were commands in the log. A snapshot may or may not have
					// been applied, but it wouldn't have created the on-disk database in that
					// case since there were commands in the log. This is the very last chance
					// to do it.
					b, err := s.db.Serialize()
					if err != nil {
						e = &fsmGenericResponse{error: fmt.Errorf("serialize failed: %s", err)}
						return
					}
					if err := s.db.Close(); err != nil {
						e = &fsmGenericResponse{error: fmt.Errorf("close failed: %s", err)}
						return
					}
					// Open a new on-disk database.
					s.db, err = s.openOnDisk(b)
					if err := ioutil.WriteFile(s.dbPath, b, 0660); err != nil {
						e = &fsmGenericResponse{error: fmt.Errorf("open on-disk failed: %s", err)}
					}
					s.onDiskCreated = true
					s.logger.Println("successfully switched to on-disk database")
				}
			}
		}
	}()

	if s.firstLogAppliedT.IsZero() {
		s.firstLogAppliedT = time.Now()
	}

	var c command.Command

	if err := legacy.Unmarshal(l.Data, &c); err != nil {
		if err2 := command.Unmarshal(l.Data, &c); err2 != nil {
			panic(fmt.Sprintf("failed to unmarshal cluster command: %s (legacy unmarshal: %s)",
				err2.Error(), err.Error()))
		}
	} else {
		stats.Add(numLegacyCommands, 1)
	}

	switch c.Type {
	case command.Command_COMMAND_TYPE_QUERY:
		var qr command.QueryRequest
		if err := command.UnmarshalSubCommand(&c, &qr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		// Read from database. If a transaction is requested, we must block
		// certain other database operations.
		if qr.Request.Transaction {
			s.txMu.Lock()
			defer s.txMu.Unlock()
		}
		r, err := s.db.Query(qr.Request, qr.Timings)
		return &fsmQueryResponse{rows: r, error: err}
	case command.Command_COMMAND_TYPE_EXECUTE:
		var er command.ExecuteRequest
		if err := command.UnmarshalSubCommand(&c, &er); err != nil {
			panic(fmt.Sprintf("failed to unmarshal execute subcommand: %s", err.Error()))
		}
		r, err := s.db.Execute(er.Request, er.Timings)
		return &fsmExecuteResponse{results: r, error: err}
	case command.Command_COMMAND_TYPE_METADATA_SET:
		var ms command.MetadataSet
		if err := command.UnmarshalSubCommand(&c, &ms); err != nil {
			panic(fmt.Sprintf("failed to unmarshal metadata set subcommand: %s", err.Error()))
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			if _, ok := s.meta[ms.RaftId]; !ok {
				s.meta[ms.RaftId] = make(map[string]string)
			}
			for k, v := range ms.Data {
				s.meta[ms.RaftId][k] = v
			}
		}()
		return &fsmGenericResponse{}
	case command.Command_COMMAND_TYPE_METADATA_DELETE:
		var md command.MetadataDelete
		if err := command.UnmarshalSubCommand(&c, &md); err != nil {
			panic(fmt.Sprintf("failed to unmarshal metadata delete subcommand: %s", err.Error()))
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			delete(s.meta, md.RaftId)
		}()
		return &fsmGenericResponse{}
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unhandled command: %v", c.Type)}
	}
}

// Database returns a copy of the underlying database. The caller MUST
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
	return s.db.Serialize()
}

// Snapshot returns a snapshot of the database. The caller must ensure that
// no transaction is taking place during this call. Hashicorp Raft guarantees
// that this function will not be called concurrently with Apply, as it states
// Apply and Snapshot are always called from the same thread. This means there
// is no need to synchronize this function with Execute(). However queries that
// involve a transaction must be blocked.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	fsm := &fsmSnapshot{
		startT: time.Now(),
		logger: s.logger,
	}

	s.txMu.Lock()
	defer s.txMu.Unlock()

	var err error
	fsm.database, err = s.db.Serialize()
	if err != nil {
		s.logger.Printf("failed to serialize database for snapshot: %s", err.Error())
		return nil, err
	}

	fsm.meta, err = json.Marshal(s.meta)
	if err != nil {
		s.logger.Printf("failed to encode meta for snapshot: %s", err.Error())
		return nil, err
	}

	stats.Add(numSnaphots, 1)
	s.logger.Printf("node snapshot created in %s", time.Since(fsm.startT))
	return fsm, nil
}

// Restore restores the node to a previous state. The Hashicorp docs state this
// will not be called concurrently with Apply(), so synchronization with Execute()
// is not necessary.To prevent problems during queries, which may not go through
// the log, it blocks all query requests.
func (s *Store) Restore(rc io.ReadCloser) error {
	startT := time.Now()

	s.queryMu.Lock()
	defer s.queryMu.Unlock()

	var uint64Size uint64
	inc := int64(unsafe.Sizeof(uint64Size))

	// Read all the data into RAM, since we have to decode known-length
	// chunks of various forms.
	var offset int64
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("readall: %s", err)
	}

	// Get size of database, checking for compression.
	compressed := false
	sz, err := readUint64(b[offset : offset+inc])
	if err != nil {
		return fmt.Errorf("read compression check: %s", err)
	}
	offset = offset + inc

	if sz == math.MaxUint64 {
		compressed = true
		// Database is actually compressed, read actual size next.
		sz, err = readUint64(b[offset : offset+inc])
		if err != nil {
			return fmt.Errorf("read compressed size: %s", err)
		}
		offset = offset + inc
	}

	// Now read in the database file data, decompress if necessary, and restore.
	var database []byte
	if compressed {
		buf := new(bytes.Buffer)
		gz, err := gzip.NewReader(bytes.NewReader(b[offset : offset+int64(sz)]))
		if err != nil {
			return err
		}

		if _, err := io.Copy(buf, gz); err != nil {
			return fmt.Errorf("SQLite database decompress: %s", err)
		}

		if err := gz.Close(); err != nil {
			return err
		}
		database = buf.Bytes()
		offset += int64(sz)
	} else {
		database = b[offset : offset+int64(sz)]
		offset += int64(sz)
	}

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close pre-restore database: %s", err)
	}

	var db *sql.DB
	if !s.dbConf.Memory && s.lastCommandIdxOnOpen == 0 {
		// A snapshot clearly exists (this function has been called) but there
		// are no command entries in the log -- so Apply will not be called.
		// Therefore this is the last opportunity to create the on-disk database
		// before Raft starts.
		db, err = s.openOnDisk(database)
		if err != nil {
			return fmt.Errorf("open on-disk file during restore: %s", err)
		}
		s.onDiskCreated = true
		s.logger.Println("successfully switched to on-disk database due to restore")
	} else {
		// Deserialize into an in-memory database because a) an in-memory database
		// has been requested, or b) while there was a snapshot, there are also
		// command entries in the log. So by sticking with an in-memory database
		// those entries will be applied in the fastest possible manner. We will
		// defer creation of any database on disk until the Apply function.
		db, err = s.openInMemory(database)
		if err != nil {
			return fmt.Errorf("openInMemory: %s", err)
		}
	}
	s.db = db

	// Unmarshal remaining bytes, and set to cluster meta.
	err = func() error {
		s.metaMu.Lock()
		defer s.metaMu.Unlock()
		return json.Unmarshal(b[offset:], &s.meta)
	}()
	if err != nil {
		return fmt.Errorf("cluster metadata unmarshal: %s", err)
	}

	stats.Add(numRestores, 1)
	s.logger.Printf("node restored in %s", time.Since(startT))
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

// logSize returns the size of the Raft log on disk.
func (s *Store) logSize() (int64, error) {
	fi, err := os.Stat(filepath.Join(s.raftDir, raftDBPath))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type fsmSnapshot struct {
	startT time.Time
	logger *log.Logger

	database []byte
	meta     []byte
}

// Persist writes the snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		f.logger.Printf("snapshot and persist took %s", time.Since(f.startT))
	}()

	err := func() error {
		b := new(bytes.Buffer)

		// Flag compressed database by writing max uint64 value first.
		// No SQLite database written by earlier versions will have this
		// as a size. *Surely*.
		err := writeUint64(b, math.MaxUint64)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b.Bytes()); err != nil {
			return err
		}
		b.Reset() // Clear state of buffer for future use.

		// Get compressed copy of database.
		cdb, err := f.compressedDatabase()
		if err != nil {
			return err
		}

		// Write size of compressed database.
		err = writeUint64(b, uint64(len(cdb)))
		if err != nil {
			return err
		}
		if _, err := sink.Write(b.Bytes()); err != nil {
			return err
		}

		// Write compressed database to sink.
		if _, err := sink.Write(cdb); err != nil {
			return err
		}

		// Write the cluster metadata.
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

func (f *fsmSnapshot) compressedDatabase() ([]byte, error) {
	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}

	if _, err := gz.Write(f.database); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Database copies contents of the underlying SQLite database to dst
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

	if err := s.db.Backup(f.Name()); err != nil {
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

func (s *Store) databaseTypePretty() string {
	if s.dbConf.Memory {
		return "in-memory"
	}
	return "on-disk"
}

// Release is a no-op.
func (f *fsmSnapshot) Release() {}

func readUint64(b []byte) (uint64, error) {
	var sz uint64
	if err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &sz); err != nil {
		return 0, err
	}
	return sz, nil
}

func writeUint64(w io.Writer, v uint64) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// enabledFromBool converts bool to "enabled" or "disabled".
func enabledFromBool(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}

// prettyVoter converts bool to "voter" or "non-voter"
func prettyVoter(v bool) string {
	if v {
		return "voter"
	}
	return "non-voter"
}

// pathExists returns true if the given path exists.
func pathExists(p string) bool {
	if _, err := os.Lstat(p); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
