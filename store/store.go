// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/command"
	sql "github.com/rqlite/rqlite/db"
	rlog "github.com/rqlite/rqlite/log"
)

var (
	// ErrNotOpen is returned when a Store is not open.
	ErrNotOpen = errors.New("store not open")

	// ErrNotReady is returned when a Store is not ready to accept requests.
	ErrNotReady = errors.New("store not ready")

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
	peersPath           = "raft/peers.json"
	peersInfoPath       = "raft/peers.info"
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
	observerChanLen     = 50
	selfLeaderChanLen   = 5
)

const (
	numSnaphots              = "num_snapshots"
	numProvides              = "num_provides"
	numBackups               = "num_backups"
	numLoads                 = "num_loads"
	numRestores              = "num_restores"
	numRecoveries            = "num_recoveries"
	numUncompressedCommands  = "num_uncompressed_commands"
	numCompressedCommands    = "num_compressed_commands"
	numJoins                 = "num_joins"
	numIgnoredJoins          = "num_ignored_joins"
	numRemovedBeforeJoins    = "num_removed_before_joins"
	snapshotCreateDuration   = "snapshot_create_duration"
	snapshotPersistDuration  = "snapshot_persist_duration"
	snapshotDBSerializedSize = "snapshot_db_serialized_size"
	snapshotDBOnDiskSize     = "snapshot_db_ondisk_size"
	leaderChangesObserved    = "leader_changes_observed"
	leaderChangesDropped     = "leader_changes_dropped"
	failedHeartbeatObserved  = "failed_heartbeat_observed"
	nodesReapedOK            = "nodes_reaped_ok"
	nodesReapedFailed        = "nodes_reaped_failed"
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("store")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Add(numSnaphots, 0)
	stats.Add(numProvides, 0)
	stats.Add(numBackups, 0)
	stats.Add(numRestores, 0)
	stats.Add(numRecoveries, 0)
	stats.Add(numUncompressedCommands, 0)
	stats.Add(numCompressedCommands, 0)
	stats.Add(numJoins, 0)
	stats.Add(numIgnoredJoins, 0)
	stats.Add(numRemovedBeforeJoins, 0)
	stats.Add(snapshotCreateDuration, 0)
	stats.Add(snapshotPersistDuration, 0)
	stats.Add(snapshotDBSerializedSize, 0)
	stats.Add(snapshotDBOnDiskSize, 0)
	stats.Add(leaderChangesObserved, 0)
	stats.Add(leaderChangesDropped, 0)
	stats.Add(failedHeartbeatObserved, 0)
	stats.Add(nodesReapedOK, 0)
	stats.Add(nodesReapedFailed, 0)
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
	open          bool
	raftDir       string
	peersPath     string
	peersInfoPath string

	raft   *raft.Raft // The consensus mechanism.
	ln     Listener
	raftTn *raft.NetworkTransport
	raftID string    // Node ID.
	dbConf *DBConfig // SQLite database config.
	dbPath string    // Path to underlying SQLite file, if not in-memory.
	db     *sql.DB   // The underlying SQLite store.

	queryTxMu sync.RWMutex

	dbAppliedIndexMu sync.RWMutex
	dbAppliedIndex   uint64

	// Channels that must be closed for the Store to be considered ready.
	readyChans             []<-chan struct{}
	numClosedReadyChannels int
	readyChansMu           sync.Mutex

	// Latest log entry index actually reflected by the FSM. Due to Raft code
	// this value is not updated after a Snapshot-restore.
	fsmIndex   uint64
	fsmIndexMu sync.RWMutex

	reqMarshaller *command.RequestMarshaler // Request marshaler for writing to log.
	raftLog       raft.LogStore             // Persistent log store.
	raftStable    raft.StableStore          // Persistent k-v store.
	boltStore     *rlog.Log                 // Physical store.

	// Raft changes observer
	leaderObserversMu sync.RWMutex
	leaderObservers   []chan<- struct{}
	observerClose     chan struct{}
	observerDone      chan struct{}
	observerChan      chan raft.Observation
	observer          *raft.Observer
	selfLeaderChan    chan struct{}
	selfLeaderClose   chan struct{}
	selfLeaderDone    chan struct{}

	onDiskCreated        bool      // On disk database actually created?
	snapsExistOnOpen     bool      // Any snaps present when store opens?
	firstIdxOnOpen       uint64    // First index on log when Store opens.
	lastIdxOnOpen        uint64    // Last index on log when Store opens.
	lastCommandIdxOnOpen uint64    // Last command index on log when Store opens.
	firstLogAppliedT     time.Time // Time first log is applied
	appliedOnOpen        uint64    // Number of logs applied at open.
	openT                time.Time // Timestamp when Store opens.

	logger *log.Logger

	notifyMu        sync.Mutex
	BootstrapExpect int
	bootstrapped    bool
	notifyingNodes  map[string]*Server

	// StartupOnDisk disables in-memory initialization of on-disk databases.
	// Restarting a node with an on-disk database can be slow so, by default,
	// rqlite creates on-disk databases in memory first, and then moves the
	// database to disk before Raft starts. However, this optimization can
	// prevent nodes with very large (2GB+) databases from starting. This
	// flag allows control of the optimization.
	StartupOnDisk bool

	ShutdownOnRemove   bool
	SnapshotThreshold  uint64
	SnapshotInterval   time.Duration
	LeaderLeaseTimeout time.Duration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	ApplyTimeout       time.Duration
	RaftLogLevel       string
	NoFreeListSync     bool

	// Node-reaping configuration
	ReapTimeout         time.Duration
	ReapReadOnlyTimeout time.Duration

	numTrailingLogs uint64

	// For whitebox testing
	numIgnoredJoins int
	numNoops        int
	numSnapshotsMu  sync.Mutex
	numSnapshots    int
}

// IsNewNode returns whether a node using raftDir would be a brand-new node.
// It also means that the window for this node joining a different cluster has passed.
func IsNewNode(raftDir string) bool {
	// If there is any pre-existing Raft state, then this node
	// has already been created.
	return !pathExists(filepath.Join(raftDir, raftDBPath))
}

// Config represents the configuration of the underlying Store.
type Config struct {
	DBConf *DBConfig   // The DBConfig object for this Store.
	Dir    string      // The working directory for raft.
	Tn     Transport   // The underlying Transport for raft.
	ID     string      // Node ID.
	Logger *log.Logger // The logger to use to log stuff.
}

// New returns a new Store.
func New(ln Listener, c *Config) *Store {
	logger := c.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[store] ", log.LstdFlags)
	}

	dbPath := filepath.Join(c.Dir, sqliteFile)
	if c.DBConf.OnDiskPath != "" {
		dbPath = c.DBConf.OnDiskPath
	}

	return &Store{
		ln:              ln,
		raftDir:         c.Dir,
		peersPath:       filepath.Join(c.Dir, peersPath),
		peersInfoPath:   filepath.Join(c.Dir, peersInfoPath),
		raftID:          c.ID,
		dbConf:          c.DBConf,
		dbPath:          dbPath,
		leaderObservers: make([]chan<- struct{}, 0),
		reqMarshaller:   command.NewRequestMarshaler(),
		logger:          logger,
		notifyingNodes:  make(map[string]*Server),
		ApplyTimeout:    applyTimeout,
	}
}

// Open opens the Store.
func (s *Store) Open() (retErr error) {
	defer func() {
		if retErr == nil {
			s.open = true
		}
	}()

	if s.open {
		return fmt.Errorf("store already open")
	}

	s.openT = time.Now()
	s.logger.Printf("opening store with node ID %s", s.raftID)

	if !s.dbConf.Memory {
		s.logger.Printf("configured for an on-disk database at %s", s.dbPath)
		s.logger.Printf("on-disk database in-memory creation %s", enabledFromBool(!s.StartupOnDisk))
		parentDir := filepath.Dir(s.dbPath)
		s.logger.Printf("ensuring directory for on-disk database exists at %s", parentDir)
		err := os.MkdirAll(parentDir, 0755)
		if err != nil {
			return err
		}
	} else {
		s.logger.Printf("configured for an in-memory database")
	}

	// Create all the required Raft directories.
	s.logger.Printf("ensuring directory for Raft exists at %s", s.raftDir)
	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.peersPath), 0755); err != nil {
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
	s.boltStore, err = rlog.New(filepath.Join(s.raftDir, raftDBPath), s.NoFreeListSync)
	if err != nil {
		return fmt.Errorf("new log store: %s", err)
	}
	s.raftStable = s.boltStore
	s.raftLog, err = raft.NewLogCache(raftLogCacheSize, s.boltStore)
	if err != nil {
		return fmt.Errorf("new cached store: %s", err)
	}

	// Request to recover node?
	if pathExists(s.peersPath) {
		s.logger.Printf("attempting node recovery using %s", s.peersPath)
		config, err := raft.ReadConfigJSON(s.peersPath)
		if err != nil {
			return fmt.Errorf("failed to read peers file: %s", err.Error())
		}
		if err = RecoverNode(s.raftDir, s.logger, s.raftLog, s.raftStable, snapshots, s.raftTn, config); err != nil {
			return fmt.Errorf("failed to recover node: %s", err.Error())
		}
		if err := os.Rename(s.peersPath, s.peersInfoPath); err != nil {
			return fmt.Errorf("failed to move %s after recovery: %s", s.peersPath, err.Error())
		}
		s.logger.Printf("node recovered successfully using %s", s.peersPath)
		stats.Add(numRecoveries, 1)
	}

	// Get some info about the log, before any more entries are committed.
	if err := s.setLogInfo(); err != nil {
		return fmt.Errorf("set log info: %s", err)
	}
	s.logger.Printf("first log index: %d, last log index: %d, last command log index: %d:",
		s.firstIdxOnOpen, s.lastIdxOnOpen, s.lastCommandIdxOnOpen)

	// If an on-disk database has been requested, and there are no snapshots, and
	// there are no commands in the log, then this is the only opportunity to
	// create that on-disk database file before Raft initializes. In addition, this
	// can also happen if the user explicitly disables the startup optimization of
	// building the SQLite database in memory, before switching to disk.
	if s.StartupOnDisk || (!s.dbConf.Memory && !s.snapsExistOnOpen && s.lastCommandIdxOnOpen == 0) {
		s.db, err = createOnDisk(nil, s.dbPath, s.dbConf.FKConstraints)
		if err != nil {
			return fmt.Errorf("failed to create on-disk database")
		}
		s.onDiskCreated = true
		s.logger.Printf("created on-disk database at open")
	} else {
		// We need an in-memory database, at least for bootstrapping purposes.
		s.db, err = createInMemory(nil, s.dbConf.FKConstraints)
		if err != nil {
			return fmt.Errorf("failed to create in-memory database")
		}
		s.logger.Printf("created in-memory database at open")
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, s, s.raftLog, s.raftStable, snapshots, s.raftTn)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	// Open the observer channels.
	s.selfLeaderChan = make(chan struct{}, selfLeaderChanLen)
	s.observerChan = make(chan raft.Observation, observerChanLen)
	s.observer = raft.NewObserver(s.observerChan, false, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		return isLeaderChange || isFailedHeartBeat
	})

	// Register and listen for leader changes.
	s.raft.RegisterObserver(s.observer)
	s.observerClose, s.observerDone = s.observe()
	s.selfLeaderClose, s.selfLeaderDone = s.observeSelfLeader()

	return nil
}

// Bootstrap executes a cluster bootstrap on this node, using the given
// Servers as the configuration.
func (s *Store) Bootstrap(servers ...*Server) error {
	raftServers := make([]raft.Server, len(servers))
	for i := range servers {
		raftServers[i] = raft.Server{
			ID:      raft.ServerID(servers[i].ID),
			Address: raft.ServerAddress(servers[i].Addr),
		}
	}
	s.raft.BootstrapCluster(raft.Configuration{
		Servers: raftServers,
	})

	return nil
}

// Stepdown forces this node to relinquish leadership to another node in
// the cluster. If this node is not the leader, and 'wait' is true, an error
// will be returned.
func (s *Store) Stepdown(wait bool) error {
	f := s.raft.LeadershipTransfer()
	if !wait {
		return nil
	}
	return f.Error()
}

// RegisterReadyChannel registers a channel that must be closed before the
// store is considered "ready" to serve requests.
func (s *Store) RegisterReadyChannel(ch <-chan struct{}) {
	s.readyChansMu.Lock()
	defer s.readyChansMu.Unlock()
	s.readyChans = append(s.readyChans, ch)
	go func() {
		<-ch
		s.readyChansMu.Lock()
		s.numClosedReadyChannels++
		s.readyChansMu.Unlock()
	}()
}

// Ready returns true if the store is ready to serve requests. Ready is
// defined as having no open channels registered via RegisterReadyChannel
// and having a Leader.
func (s *Store) Ready() bool {
	l, err := s.LeaderAddr()
	if err != nil || l == "" {
		return false
	}

	return func() bool {
		s.readyChansMu.Lock()
		defer s.readyChansMu.Unlock()
		if s.numClosedReadyChannels != len(s.readyChans) {
			return false
		}
		s.readyChans = nil
		s.numClosedReadyChannels = 0
		return true
	}()
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
func (s *Store) Close(wait bool) (retErr error) {
	defer func() {
		if retErr == nil {
			s.open = false
		}
	}()
	if !s.open {
		// Protect against closing already-closed resource, such as channels.
		return nil
	}

	close(s.observerClose)
	<-s.observerDone
	close(s.selfLeaderClose)
	<-s.selfLeaderDone

	f := s.raft.Shutdown()
	if wait {
		if f.Error() != nil {
			return f.Error()
		}
	}
	// Only shutdown Bolt and SQLite when Raft is done.
	if err := s.db.Close(); err != nil {
		return err
	}
	if err := s.boltStore.Close(); err != nil {
		return err
	}

	return nil
}

// WaitForAppliedFSM waits until the currently applied logs (at the time this
// function is called) are actually reflected by the FSM, or the timeout expires.
func (s *Store) WaitForAppliedFSM(timeout time.Duration) (uint64, error) {
	if timeout == 0 {
		return 0, nil
	}
	return s.WaitForFSMIndex(s.raft.AppliedIndex(), timeout)
}

// WaitForInitialLogs waits for logs that were in the Store at time of open
// to be applied to the state machine.
func (s *Store) WaitForInitialLogs(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	s.logger.Printf("waiting for up to %s for application of initial logs (lcIdx=%d)",
		timeout, s.lastCommandIdxOnOpen)
	return s.WaitForApplied(timeout)
}

// WaitForApplied waits for all Raft log entries to be applied to the
// underlying database.
func (s *Store) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	return s.WaitForAppliedIndex(s.raft.LastIndex(), timeout)
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

// IsLeader is used to determine if the current node is cluster leader
func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// IsVoter returns true if the current node is a voter in the cluster. If there
// is no reference to the current node in the current cluster configuration then
// false will also be returned.
func (s *Store) IsVoter() (bool, error) {
	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false, err
	}
	for _, srv := range cfg.Configuration().Servers {
		if srv.ID == raft.ServerID(s.raftID) {
			return srv.Suffrage == raft.Voter, nil
		}
	}
	return false, nil
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
// blank string if there is no leader or if the Store is not open.
func (s *Store) LeaderAddr() (string, error) {
	if !s.open {
		return "", nil
	}

	return string(s.raft.Leader()), nil
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	addr, err := s.LeaderAddr()
	if err != nil {
		return "", nil
	}
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
	if !s.open {
		return nil, ErrNotOpen
	}

	f := s.raft.GetConfiguration()
	if f.Error() != nil {
		return nil, f.Error()
	}

	rs := f.Configuration().Servers
	servers := make([]*Server, len(rs))
	for i := range rs {
		servers[i] = &Server{
			ID:       string(rs[i].ID),
			Addr:     string(rs[i].Address),
			Suffrage: rs[i].Suffrage.String(),
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
			l, err := s.LeaderAddr()
			if err != nil {
				return "", nil
			}
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

// WaitForFSMIndex blocks until a given log index has been applied to the
// state machine or the timeout expires.
func (s *Store) WaitForFSMIndex(idx uint64, timeout time.Duration) (uint64, error) {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	var fsmIdx uint64
	for {
		select {
		case <-tck.C:
			s.fsmIndexMu.RLock()
			fsmIdx = s.fsmIndex
			s.fsmIndexMu.RUnlock()
			if fsmIdx >= idx {
				return fsmIdx, nil
			}
		case <-tmr.C:
			return 0, fmt.Errorf("timeout expired")
		}
	}
}

// Stats returns stats for the store.
func (s *Store) Stats() (map[string]interface{}, error) {
	if !s.open {
		return map[string]interface{}{
			"open": false,
		}, nil
	}

	fsmIdx := func() uint64 {
		s.fsmIndexMu.RLock()
		defer s.fsmIndexMu.RUnlock()
		return s.fsmIndex
	}()
	dbAppliedIdx := func() uint64 {
		s.dbAppliedIndexMu.Lock()
		defer s.dbAppliedIndexMu.Unlock()
		return s.dbAppliedIndex
	}()
	dbStatus, err := s.db.Stats()
	if err != nil {
		return nil, err
	}

	nodes, err := s.Nodes()
	if err != nil {
		return nil, err
	}
	leaderID, err := s.LeaderID()
	if err != nil {
		return nil, err
	}

	// Perform type-conversion to actual numbers where possible.
	raftStats := make(map[string]interface{})
	for k, v := range s.raft.Stats() {
		if s, err := strconv.ParseInt(v, 10, 64); err != nil {
			raftStats[k] = v
		} else {
			raftStats[k] = s
		}
	}
	raftStats["log_size"], err = s.logSize()
	if err != nil {
		return nil, err
	}
	raftStats["voter"], err = s.IsVoter()
	if err != nil {
		return nil, err
	}
	raftStats["bolt"] = s.boltStore.Stats()

	dirSz, err := dirSize(s.raftDir)
	if err != nil {
		return nil, err
	}

	leaderAddr, err := s.LeaderAddr()
	if err != nil {
		return nil, err
	}
	status := map[string]interface{}{
		"open":             s.open,
		"node_id":          s.raftID,
		"raft":             raftStats,
		"fsm_index":        fsmIdx,
		"db_applied_index": dbAppliedIdx,
		"addr":             s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    leaderAddr,
		},
		"ready": s.Ready(),
		"observer": map[string]uint64{
			"observed": s.observer.GetNumObserved(),
			"dropped":  s.observer.GetNumDropped(),
		},
		"startup_on_disk":        s.StartupOnDisk,
		"apply_timeout":          s.ApplyTimeout.String(),
		"heartbeat_timeout":      s.HeartbeatTimeout.String(),
		"election_timeout":       s.ElectionTimeout.String(),
		"snapshot_threshold":     s.SnapshotThreshold,
		"snapshot_interval":      s.SnapshotInterval.String(),
		"reap_timeout":           s.ReapTimeout.String(),
		"reap_read_only_timeout": s.ReapReadOnlyTimeout.String(),
		"no_freelist_sync":       s.NoFreeListSync,
		"trailing_logs":          s.numTrailingLogs,
		"request_marshaler":      s.reqMarshaller.Stats(),
		"nodes":                  nodes,
		"dir":                    s.raftDir,
		"dir_size":               dirSz,
		"sqlite3":                dbStatus,
		"db_conf":                s.dbConf,
	}
	return status, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(ex *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
	if !s.open {
		return nil, ErrNotOpen
	}

	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	if !s.Ready() {
		return nil, ErrNotReady
	}

	return s.execute(ex)
}

func (s *Store) execute(ex *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
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

	af := s.raft.Apply(b, s.ApplyTimeout)
	if af.Error() != nil {
		if af.Error() == raft.ErrNotLeader {
			return nil, ErrNotLeader
		}
		return nil, af.Error()
	}

	s.dbAppliedIndexMu.Lock()
	s.dbAppliedIndex = af.Index()
	s.dbAppliedIndexMu.Unlock()
	r := af.Response().(*fsmExecuteResponse)
	return r.results, r.error
}

// Query executes queries that return rows, and do not modify the database.
func (s *Store) Query(qr *command.QueryRequest) ([]*command.QueryRows, error) {
	if !s.open {
		return nil, ErrNotOpen
	}

	if qr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG {
		if s.raft.State() != raft.Leader {
			return nil, ErrNotLeader
		}

		if !s.Ready() {
			return nil, ErrNotReady
		}

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

		af := s.raft.Apply(b, s.ApplyTimeout)
		if af.Error() != nil {
			if af.Error() == raft.ErrNotLeader {
				return nil, ErrNotLeader
			}
			return nil, af.Error()
		}

		s.dbAppliedIndexMu.Lock()
		s.dbAppliedIndex = af.Index()
		s.dbAppliedIndexMu.Unlock()
		r := af.Response().(*fsmQueryResponse)
		return r.rows, r.error
	}

	if qr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	if s.raft.State() != raft.Leader && qr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_NONE &&
		qr.Freshness > 0 && time.Since(s.raft.LastContact()).Nanoseconds() > qr.Freshness {
		return nil, ErrStaleRead
	}

	if qr.Request.Transaction {
		// Transaction requested during query, but not going through consensus. This means
		// we need to block any database serialization during the query.
		s.queryTxMu.RLock()
		defer s.queryTxMu.RUnlock()
	}

	return s.db.Query(qr.Request, qr.Timings)
}

// Backup writes a snapshot of the underlying database to dst
//
// If Leader is true for the request, this operation is performed with a read consistency
// level equivalent to "weak". Otherwise, no guarantees are made about the read consistency
// level. This function is safe to call while the database is being changed.
func (s *Store) Backup(br *command.BackupRequest, dst io.Writer) (retErr error) {
	if !s.open {
		return ErrNotOpen
	}

	startT := time.Now()
	defer func() {
		if retErr == nil {
			stats.Add(numBackups, 1)
			s.logger.Printf("database backed up in %s", time.Since(startT))
		}
	}()

	if br.Leader && s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	if br.Format == command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
		f, err := os.CreateTemp("", "rqlite-snap-")
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		defer os.Remove(f.Name())

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
	} else if br.Format == command.BackupRequest_BACKUP_REQUEST_FORMAT_SQL {
		return s.db.Dump(dst)
	}
	return ErrInvalidBackupFormat
}

// Provide implements the uploader Provider interface, allowing the
// Store to be used as a DataProvider for an uploader.
func (s *Store) Provide(path string) error {
	if err := s.db.Backup(path); err != nil {
		return err
	}
	stats.Add(numProvides, 1)
	return nil
}

// Loads an entire SQLite file into the database, sending the request
// through the Raft log.
func (s *Store) Load(lr *command.LoadRequest) error {
	if !s.open {
		return ErrNotOpen
	}

	if !s.Ready() {
		return ErrNotReady
	}

	startT := time.Now()

	b, err := command.MarshalLoadRequest(lr)
	if err != nil {
		s.logger.Printf("load failed during load-request marshalling %s", err.Error())
		return err
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_LOAD,
		SubCommand: b,
	}

	b, err = command.Marshal(c)
	if err != nil {
		return err
	}

	af := s.raft.Apply(b, s.ApplyTimeout)
	if af.Error() != nil {
		if af.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		s.logger.Printf("load failed during Apply: %s", af.Error())
		return af.Error()
	}

	s.dbAppliedIndexMu.Lock()
	s.dbAppliedIndex = af.Index()
	s.dbAppliedIndexMu.Unlock()
	stats.Add(numLoads, 1)
	s.logger.Printf("node loaded with in %s (%d bytes)", time.Since(startT), len(b))
	return af.Error()
}

// Notify notifies this Store that a node is ready for bootstrapping at the
// given address. Once the number of known nodes reaches the expected level
// bootstrapping will be attempted using this Store. "Expected level" includes
// this node, so this node must self-notify to ensure the cluster bootstraps
// with the *advertised Raft address* which the Store doesn't know about.
//
// Notifying is idempotent. A node may repeatedly notify the Store without issue.
func (s *Store) Notify(nr *command.NotifyRequest) error {
	if !s.open {
		return ErrNotOpen
	}

	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()

	if s.BootstrapExpect == 0 || s.bootstrapped || s.raft.Leader() != "" {
		// There is no reason this node will bootstrap.
		return nil
	}

	if _, ok := s.notifyingNodes[nr.Id]; ok {
		return nil
	}
	s.notifyingNodes[nr.Id] = &Server{nr.Id, nr.Address, "voter"}
	if len(s.notifyingNodes) < s.BootstrapExpect {
		return nil
	}

	raftServers := make([]raft.Server, 0)
	for _, n := range s.notifyingNodes {
		raftServers = append(raftServers, raft.Server{
			ID:      raft.ServerID(n.ID),
			Address: raft.ServerAddress(n.Addr),
		})
	}

	s.logger.Printf("reached expected bootstrap count of %d, starting cluster bootstrap",
		s.BootstrapExpect)
	bf := s.raft.BootstrapCluster(raft.Configuration{
		Servers: raftServers,
	})
	if bf.Error() != nil {
		s.logger.Printf("cluster bootstrap failed: %s", bf.Error())
	} else {
		s.logger.Printf("cluster bootstrap successful, servers: %s", raftServers)
	}
	s.bootstrapped = true
	return nil
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(jr *command.JoinRequest) error {
	if !s.open {
		return ErrNotOpen
	}

	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	id := jr.Id
	addr := jr.Address
	voter := jr.Voter

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(id) || srv.Address == raft.ServerAddress(addr) {
			// However, if *both* the ID and the address are the same, then no
			// join is actually needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(id) {
				stats.Add(numIgnoredJoins, 1)
				s.numIgnoredJoins++
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", id, addr)
				return nil
			}

			if err := s.remove(id); err != nil {
				s.logger.Printf("failed to remove node %s: %v", id, err)
				return err
			}
			stats.Add(numRemovedBeforeJoins, 1)
			s.logger.Printf("removed node %s prior to rejoin with changed ID or address", id)
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

	stats.Add(numJoins, 1)
	s.logger.Printf("node with ID %s, at %s, joined successfully as %s", id, addr, prettyVoter(voter))
	return nil
}

// Remove removes a node from the store.
func (s *Store) Remove(rn *command.RemoveNodeRequest) error {
	if !s.open {
		return ErrNotOpen
	}
	id := rn.Id

	s.logger.Printf("received request to remove node %s", id)
	if err := s.remove(id); err != nil {
		return err
	}

	s.logger.Printf("node %s removed successfully", id)
	return nil
}

// Noop writes a noop command to the Raft log. A noop command simply
// consumes a slot in the Raft log, but has no other effect on the
// system.
func (s *Store) Noop(id string) error {
	n := &command.Noop{
		Id: id,
	}
	b, err := command.MarshalNoop(n)
	if err != nil {
		return err
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_NOOP,
		SubCommand: b,
	}
	bc, err := command.Marshal(c)
	if err != nil {
		return err
	}

	af := s.raft.Apply(bc, s.ApplyTimeout)
	if af.Error() != nil {
		if af.Error() == raft.ErrNotLeader {
			return ErrNotLeader
		}
		return af.Error()
	}
	return nil
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
	results []*command.ExecuteResult
	error   error
}

type fsmQueryResponse struct {
	rows  []*command.QueryRows
	error error
}

type fsmGenericResponse struct {
	error error
}

// Apply applies a Raft log entry to the database.
func (s *Store) Apply(l *raft.Log) (e interface{}) {
	defer func() {
		s.fsmIndexMu.Lock()
		defer s.fsmIndexMu.Unlock()
		s.fsmIndex = l.Index

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
				} else if s.onDiskCreated {
					s.logger.Println("continuing use of on-disk database")
				} else {
					// Since we're here, it means that a) an on-disk database was requested,
					// b) in-memory creation of the on-disk database is enabled, and c) there
					// were commands in the log. A snapshot may or may not have been applied,
					// but it wouldn't have created the on-disk database in that case since
					// there were commands in the log. This is the very last chance to convert
					// from in-memory to on-disk.
					s.queryTxMu.Lock()
					defer s.queryTxMu.Unlock()
					b, _ := s.db.Serialize()
					err := s.db.Close()
					if err != nil {
						e = &fsmGenericResponse{error: fmt.Errorf("close failed: %s", err)}
						return
					}
					// Open a new on-disk database.
					s.db, err = createOnDisk(b, s.dbPath, s.dbConf.FKConstraints)
					if err != nil {
						e = &fsmGenericResponse{error: fmt.Errorf("open on-disk failed: %s", err)}
						return
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

	typ, r := applyCommand(l.Data, &s.db)
	if typ == command.Command_COMMAND_TYPE_NOOP {
		s.numNoops++
	}
	return r
}

// Database returns a copy of the underlying database. The caller MUST
// ensure that no transaction is taking place during this call, or an error may
// be returned. If leader is true, this operation is performed with a read
// consistency level equivalent to "weak". Otherwise, no guarantees are made
// about the read consistency level.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as the database is not written to during the call.
func (s *Store) Database(leader bool) ([]byte, error) {
	if leader && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	return s.db.Serialize()
}

// Snapshot returns a snapshot of the database.
//
// The system must ensure that no transaction is taking place during this call.
// Hashicorp Raft guarantees that this function will not be called concurrently
// with Apply, as it states Apply() and Snapshot() are always called from the same
// thread. This means there is no need to synchronize this function with Execute().
// However, queries that involve a transaction must be blocked.
//
// http://sqlite.org/howtocorrupt.html states it is safe to copy or serialize the
// database as long as no writes to the database are in progress.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	defer func() {
		s.numSnapshotsMu.Lock()
		defer s.numSnapshotsMu.Unlock()
		s.numSnapshots++
	}()

	s.queryTxMu.Lock()
	defer s.queryTxMu.Unlock()
	fsm := newFSMSnapshot(s.db, s.logger)
	dur := time.Since(fsm.startT)
	stats.Add(numSnaphots, 1)
	stats.Get(snapshotCreateDuration).(*expvar.Int).Set(dur.Milliseconds())
	stats.Get(snapshotDBSerializedSize).(*expvar.Int).Set(int64(len(fsm.database)))
	s.logger.Printf("node snapshot created in %s", dur)
	return fsm, nil
}

// Restore restores the node to a previous state. The Hashicorp docs state this
// will not be called concurrently with Apply(), so synchronization with Execute()
// is not necessary.To prevent problems during queries, which may not go through
// the log, it blocks all query requests.
func (s *Store) Restore(rc io.ReadCloser) error {
	startT := time.Now()
	b, err := dbBytesFromSnapshot(rc)
	if err != nil {
		return fmt.Errorf("restore failed: %s", err.Error())
	}
	if b == nil {
		s.logger.Println("no database data present in restored snapshot")
	}

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close pre-restore database: %s", err)
	}

	var db *sql.DB
	if s.StartupOnDisk || (!s.dbConf.Memory && s.lastCommandIdxOnOpen == 0) {
		// A snapshot clearly exists (this function has been called) but there
		// are no command entries in the log -- so Apply will not be called.
		// Therefore, this is the last opportunity to create the on-disk database
		// before Raft starts. This could also happen because the user has explicitly
		// disabled the build-on-disk-database-in-memory-first optimization.
		db, err = createOnDisk(b, s.dbPath, s.dbConf.FKConstraints)
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
		db, err = createInMemory(b, s.dbConf.FKConstraints)
		if err != nil {
			return fmt.Errorf("createInMemory: %s", err)
		}
	}
	s.db = db

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

// RegisterLeaderChange registers the given channel which will
// receive a signal when this node detects that the Leader changes.
func (s *Store) RegisterLeaderChange(c chan<- struct{}) {
	s.leaderObserversMu.Lock()
	defer s.leaderObserversMu.Unlock()
	s.leaderObservers = append(s.leaderObservers, c)
}

func (s *Store) observe() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})

	go func() {
		defer close(doneCh)
		for {
			select {
			case o := <-s.observerChan:
				switch signal := o.Data.(type) {
				case raft.FailedHeartbeatObservation:
					stats.Add(failedHeartbeatObserved, 1)

					nodes, err := s.Nodes()
					if err != nil {
						s.logger.Printf("failed to get nodes configuration during reap check: %s", err.Error())
					}
					servers := Servers(nodes)
					id := string(signal.PeerID)
					dur := time.Since(signal.LastContact)

					isReadOnly, found := servers.IsReadOnly(id)
					if !found {
						s.logger.Printf("node %s is not present in configuration", id)
						break
					}

					if (isReadOnly && s.ReapReadOnlyTimeout > 0 && dur > s.ReapReadOnlyTimeout) ||
						(!isReadOnly && s.ReapTimeout > 0 && dur > s.ReapTimeout) {
						pn := "voting node"
						if isReadOnly {
							pn = "non-voting node"
						}
						if err := s.remove(id); err != nil {
							stats.Add(nodesReapedFailed, 1)
							s.logger.Printf("failed to reap %s %s: %s", pn, id, err.Error())
						} else {
							stats.Add(nodesReapedOK, 1)
							s.logger.Printf("successfully reaped %s %s", pn, id)
						}
					}
				case raft.LeaderObservation:
					s.leaderObserversMu.RLock()
					for i := range s.leaderObservers {
						select {
						case s.leaderObservers[i] <- struct{}{}:
							stats.Add(leaderChangesObserved, 1)
						default:
							stats.Add(leaderChangesDropped, 1)
						}
					}
					s.leaderObserversMu.RUnlock()
					if signal.LeaderID == raft.ServerID(s.raftID) {
						s.selfLeaderChan <- struct{}{}
					}
				}

			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

func (s *Store) observeSelfLeader() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})

	go func() {
		defer close(doneCh)
		for {
			select {
			case <-s.selfLeaderChan:
				s.logger.Printf("this node is now the leader")
			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
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
}

func newFSMSnapshot(db *sql.DB, logger *log.Logger) *fsmSnapshot {
	fsm := &fsmSnapshot{
		startT: time.Now(),
		logger: logger,
	}

	// The error code is not meaningful from Serialize(). The code needs to be able
	// to handle a nil byte slice being returned.
	fsm.database, _ = db.Serialize()
	return fsm
}

// Persist writes the snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		dur := time.Since(f.startT)
		stats.Get(snapshotPersistDuration).(*expvar.Int).Set(dur.Milliseconds())
		f.logger.Printf("snapshot and persist took %s", dur)
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

		if cdb != nil {
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
			stats.Get(snapshotDBOnDiskSize).(*expvar.Int).Set(int64(len(cdb)))
		} else {
			f.logger.Println("no database data available for snapshot")
			err = writeUint64(b, uint64(0))
			if err != nil {
				return err
			}
			if _, err := sink.Write(b.Bytes()); err != nil {
				return err
			}
			stats.Get(snapshotDBOnDiskSize).(*expvar.Int).Set(0)
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
	if f.database == nil {
		return nil, nil
	}

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

// Release is a no-op.
func (f *fsmSnapshot) Release() {}

// RecoverNode is used to manually force a new configuration, in the event that
// quorum cannot be restored. This borrows heavily from RecoverCluster functionality
// of the Hashicorp Raft library, but has been customized for rqlite use.
func RecoverNode(dataDir string, logger *log.Logger, logs raft.LogStore, stable raft.StableStore,
	snaps raft.SnapshotStore, tn raft.Transport, conf raft.Configuration) error {
	logPrefix := logger.Prefix()
	logger.SetPrefix(fmt.Sprintf("%s[recovery] ", logPrefix))
	defer logger.SetPrefix(logPrefix)

	// Sanity check the Raft peer configuration.
	if err := checkRaftConfiguration(conf); err != nil {
		return err
	}

	// Attempt to restore any snapshots we find, newest to oldest.
	var (
		snapshotIndex  uint64
		snapshotTerm   uint64
		snapshots, err = snaps.List()
	)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %v", err)
	}
	logger.Printf("recovery detected %d snapshots", len(snapshots))

	var b []byte
	for _, snapshot := range snapshots {
		var source io.ReadCloser
		_, source, err = snaps.Open(snapshot.ID)
		if err != nil {
			// Skip this one and try the next. We will detect if we
			// couldn't open any snapshots.
			continue
		}

		b, err = dbBytesFromSnapshot(source)
		// Close the source after the restore has completed
		source.Close()
		if err != nil {
			// Same here, skip and try the next one.
			continue
		}

		snapshotIndex = snapshot.Index
		snapshotTerm = snapshot.Term
		break
	}
	if len(snapshots) > 0 && (snapshotIndex == 0 || snapshotTerm == 0) {
		return fmt.Errorf("failed to restore any of the available snapshots")
	}

	// Now, create an in-memory database for temporary use, so we can generate new
	// snapshots later.
	var db *sql.DB
	if len(b) == 0 {
		db, err = sql.OpenInMemory(false)
	} else {
		db, err = sql.DeserializeIntoMemory(b, false)
	}
	if err != nil {
		return fmt.Errorf("create in-memory database failed: %s", err)
	}
	defer db.Close()

	// The snapshot information is the best known end point for the data
	// until we play back the Raft log entries.
	lastIndex := snapshotIndex
	lastTerm := snapshotTerm

	// Apply any Raft log entries past the snapshot.
	lastLogIndex, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to find last log: %v", err)
	}
	logger.Printf("recovery snapshot index is %d, last index is %d", snapshotIndex, lastLogIndex)

	for index := snapshotIndex + 1; index <= lastLogIndex; index++ {
		var entry raft.Log
		if err = logs.GetLog(index, &entry); err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", index, err)
		}
		if entry.Type == raft.LogCommand {
			applyCommand(entry.Data, &db)
		}
		lastIndex = entry.Index
		lastTerm = entry.Term
	}

	// Create a new snapshot, placing the configuration in as if it was
	// committed at index 1.
	snapshot := newFSMSnapshot(db, logger)
	sink, err := snaps.Create(1, lastIndex, lastTerm, conf, 1, tn)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	if err = snapshot.Persist(sink); err != nil {
		return fmt.Errorf("failed to persist snapshot: %v", err)
	}
	if err = sink.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}
	logger.Printf("recovery snapshot created successfully")

	// Compact the log so that we don't get bad interference from any
	// configuration change log entries that might be there.
	firstLogIndex, err := logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}
	if err := logs.DeleteRange(firstLogIndex, lastLogIndex); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}

	return nil
}

func dbBytesFromSnapshot(rc io.ReadCloser) ([]byte, error) {
	var uint64Size uint64
	inc := int64(unsafe.Sizeof(uint64Size))

	// Read all the data into RAM, since we have to decode known-length
	// chunks of various forms.
	var offset int64
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("readall: %s", err)
	}

	// Get size of database, checking for compression.
	compressed := false
	sz, err := readUint64(b[offset : offset+inc])
	if err != nil {
		return nil, fmt.Errorf("read compression check: %s", err)
	}
	offset = offset + inc

	if sz == math.MaxUint64 {
		compressed = true
		// Database is actually compressed, read actual size next.
		sz, err = readUint64(b[offset : offset+inc])
		if err != nil {
			return nil, fmt.Errorf("read compressed size: %s", err)
		}
		offset = offset + inc
	}

	// Now read in the database file data, decompress if necessary, and restore.
	var database []byte
	if sz > 0 {
		if compressed {
			buf := new(bytes.Buffer)
			gz, err := gzip.NewReader(bytes.NewReader(b[offset : offset+int64(sz)]))
			if err != nil {
				return nil, err
			}

			if _, err := io.Copy(buf, gz); err != nil {
				return nil, fmt.Errorf("SQLite database decompress: %s", err)
			}

			if err := gz.Close(); err != nil {
				return nil, err
			}
			database = buf.Bytes()
		} else {
			database = b[offset : offset+int64(sz)]
		}
	} else {
		database = nil
	}
	return database, nil
}

func applyCommand(data []byte, pDB **sql.DB) (command.Command_Type, interface{}) {
	var c command.Command
	db := *pDB

	if err := command.Unmarshal(data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
	}

	switch c.Type {
	case command.Command_COMMAND_TYPE_QUERY:
		var qr command.QueryRequest
		if err := command.UnmarshalSubCommand(&c, &qr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		r, err := db.Query(qr.Request, qr.Timings)
		return c.Type, &fsmQueryResponse{rows: r, error: err}
	case command.Command_COMMAND_TYPE_EXECUTE:
		var er command.ExecuteRequest
		if err := command.UnmarshalSubCommand(&c, &er); err != nil {
			panic(fmt.Sprintf("failed to unmarshal execute subcommand: %s", err.Error()))
		}
		r, err := db.Execute(er.Request, er.Timings)
		return c.Type, &fsmExecuteResponse{results: r, error: err}
	case command.Command_COMMAND_TYPE_LOAD:
		var lr command.LoadRequest
		if err := command.UnmarshalLoadRequest(c.SubCommand, &lr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal load subcommand: %s", err.Error()))
		}

		var newDB *sql.DB
		var err error
		if db.InMemory() {
			newDB, err = createInMemory(lr.Data, db.FKEnabled())
			if err != nil {
				return c.Type, &fsmGenericResponse{error: fmt.Errorf("failed to create in-memory database: %s", err)}
			}
		} else {
			newDB, err = createOnDisk(lr.Data, db.Path(), db.FKEnabled())
			if err != nil {
				return c.Type, &fsmGenericResponse{error: fmt.Errorf("failed to create on-disk database: %s", err)}
			}
		}

		// Swap the underlying database to the new one.
		if err := db.Close(); err != nil {
			return c.Type, &fsmGenericResponse{error: fmt.Errorf("failed to close post-load database: %s", err)}
		}
		*pDB = newDB
		return c.Type, &fsmGenericResponse{}
	case command.Command_COMMAND_TYPE_NOOP:
		return c.Type, &fsmGenericResponse{}
	default:
		return c.Type, &fsmGenericResponse{error: fmt.Errorf("unhandled command: %v", c.Type)}
	}
}

// checkRaftConfiguration tests a cluster membership configuration for common
// errors.
func checkRaftConfiguration(configuration raft.Configuration) error {
	idSet := make(map[raft.ServerID]bool)
	addressSet := make(map[raft.ServerAddress]bool)
	var voters int
	for _, server := range configuration.Servers {
		if server.ID == "" {
			return fmt.Errorf("empty ID in configuration: %v", configuration)
		}
		if server.Address == "" {
			return fmt.Errorf("empty address in configuration: %v", server)
		}
		if strings.Contains(string(server.Address), "://") {
			return fmt.Errorf("protocol specified in address: %v", server.Address)
		}
		_, _, err := net.SplitHostPort(string(server.Address))
		if err != nil {
			return fmt.Errorf("invalid address in configuration: %v", server.Address)
		}
		if idSet[server.ID] {
			return fmt.Errorf("found duplicate ID in configuration: %v", server.ID)
		}
		idSet[server.ID] = true
		if addressSet[server.Address] {
			return fmt.Errorf("found duplicate address in configuration: %v", server.Address)
		}
		addressSet[server.Address] = true
		if server.Suffrage == raft.Voter {
			voters++
		}
	}
	if voters == 0 {
		return fmt.Errorf("need at least one voter in configuration: %v", configuration)
	}
	return nil
}

// createInMemory returns an in-memory database. If b is non-nil and non-empty,
// then the database will be initialized with the contents of b.
func createInMemory(b []byte, fkConstraints bool) (db *sql.DB, err error) {
	if len(b) == 0 {
		db, err = sql.OpenInMemory(fkConstraints)
	} else {
		db, err = sql.DeserializeIntoMemory(b, fkConstraints)
	}
	return
}

// createOnDisk opens an on-disk database file at the configured path. If b is
// non-nil, any preexisting file will first be overwritten with those contents.
// Otherwise, any preexisting file will be removed before the database is opened.
func createOnDisk(b []byte, path string, fkConstraints bool) (*sql.DB, error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if b != nil {
		if err := ioutil.WriteFile(path, b, 0660); err != nil {
			return nil, err
		}
	}
	return sql.Open(path, fkConstraints)
}

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

// dirSize returns the total size of all files in the given directory
func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			// If the file doesn't exist, we can ignore it. Snapshot files might
			// disappear during walking.
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
