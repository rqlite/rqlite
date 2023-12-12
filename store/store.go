// Package store provides a distributed SQLite instance.
//
// Distributed consensus is provided via the Raft algorithm.
package store

import (
	"bytes"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/chunking"
	sql "github.com/rqlite/rqlite/db"
	wal "github.com/rqlite/rqlite/db/wal"
	rlog "github.com/rqlite/rqlite/log"
	"github.com/rqlite/rqlite/snapshot"
)

var (
	// ErrNotOpen is returned when a Store is not open.
	ErrNotOpen = errors.New("store not open")

	// ErrOpen is returned when a Store is already open.
	ErrOpen = errors.New("store already open")

	// ErrNotReady is returned when a Store is not ready to accept requests.
	ErrNotReady = errors.New("store not ready")

	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrNotSingleNode is returned when a node attempts to execute a single-node
	// only operation.
	ErrNotSingleNode = errors.New("not single-node")

	// ErrStaleRead is returned if the executing the query would violate the
	// requested freshness.
	ErrStaleRead = errors.New("stale read")

	// ErrOpenTimeout is returned when the Store does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrWaitForRemovalTimeout is returned when the Store does not confirm removal
	// of a node within the specified time.
	ErrWaitForRemovalTimeout = errors.New("timeout waiting for node removal confirmation")

	// ErrWaitForLeaderTimeout is returned when the Store cannot determine the leader
	// within the specified time.
	ErrWaitForLeaderTimeout = errors.New("timeout waiting for leader")

	// ErrInvalidBackupFormat is returned when the requested backup format
	// is not valid.
	ErrInvalidBackupFormat = errors.New("invalid backup format")

	// ErrInvalidVacuumFormat is returned when the requested backup format is not
	// compatible with vacuum.
	ErrInvalidVacuum = errors.New("invalid vacuum")

	// ErrLoadInProgress is returned when a load is already in progress and the
	// requested operation cannot be performed.
	ErrLoadInProgress = errors.New("load in progress")
)

const (
	restoreScratchPattern      = "rqlite-restore-*"
	raftDBPath                 = "raft.db" // Changing this will break backwards compatibility.
	peersPath                  = "raft/peers.json"
	peersInfoPath              = "raft/peers.info"
	retainSnapshotCount        = 1
	applyTimeout               = 10 * time.Second
	openTimeout                = 120 * time.Second
	sqliteFile                 = "db.sqlite"
	leaderWaitDelay            = 100 * time.Millisecond
	appliedWaitDelay           = 100 * time.Millisecond
	appliedIndexUpdateInterval = 5 * time.Second
	connectionPoolCount        = 5
	connectionTimeout          = 10 * time.Second
	raftLogCacheSize           = 512
	trailingScale              = 1.25
	observerChanLen            = 50

	defaultChunkSize = 64 * 1024 * 1024 // 64 MB
)

const (
	numSnapshots              = "num_snapshots"
	numUserSnapshots          = "num_user_snapshots"
	numSnapshotsFull          = "num_snapshots_full"
	numSnapshotsIncremental   = "num_snapshots_incremental"
	numProvides               = "num_provides"
	numBackups                = "num_backups"
	numLoads                  = "num_loads"
	numLoadsBypass            = "num_loads_bypass"
	numRestores               = "num_restores"
	numAutoRestores           = "num_auto_restores"
	numAutoRestoresSkipped    = "num_auto_restores_skipped"
	numAutoRestoresFailed     = "num_auto_restores_failed"
	numRecoveries             = "num_recoveries"
	numUncompressedCommands   = "num_uncompressed_commands"
	numCompressedCommands     = "num_compressed_commands"
	numJoins                  = "num_joins"
	numIgnoredJoins           = "num_ignored_joins"
	numRemovedBeforeJoins     = "num_removed_before_joins"
	numDBStatsErrors          = "num_db_stats_errors"
	snapshotCreateDuration    = "snapshot_create_duration"
	snapshotPersistDuration   = "snapshot_persist_duration"
	snapshotPrecompactWALSize = "snapshot_precompact_wal_size"
	snapshotWALSize           = "snapshot_wal_size"
	leaderChangesObserved     = "leader_changes_observed"
	leaderChangesDropped      = "leader_changes_dropped"
	failedHeartbeatObserved   = "failed_heartbeat_observed"
	nodesReapedOK             = "nodes_reaped_ok"
	nodesReapedFailed         = "nodes_reaped_failed"
)

// stats captures stats for the Store.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("store")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numSnapshots, 0)
	stats.Add(numUserSnapshots, 0)
	stats.Add(numSnapshotsFull, 0)
	stats.Add(numSnapshotsIncremental, 0)
	stats.Add(numProvides, 0)
	stats.Add(numBackups, 0)
	stats.Add(numLoads, 0)
	stats.Add(numLoadsBypass, 0)
	stats.Add(numRestores, 0)
	stats.Add(numRecoveries, 0)
	stats.Add(numAutoRestores, 0)
	stats.Add(numAutoRestoresSkipped, 0)
	stats.Add(numAutoRestoresFailed, 0)
	stats.Add(numUncompressedCommands, 0)
	stats.Add(numCompressedCommands, 0)
	stats.Add(numJoins, 0)
	stats.Add(numIgnoredJoins, 0)
	stats.Add(numRemovedBeforeJoins, 0)
	stats.Add(numDBStatsErrors, 0)
	stats.Add(snapshotCreateDuration, 0)
	stats.Add(snapshotPersistDuration, 0)
	stats.Add(snapshotPrecompactWALSize, 0)
	stats.Add(snapshotWALSize, 0)
	stats.Add(leaderChangesObserved, 0)
	stats.Add(leaderChangesDropped, 0)
	stats.Add(failedHeartbeatObserved, 0)
	stats.Add(nodesReapedOK, 0)
	stats.Add(nodesReapedFailed, 0)
}

// SnapshotStore is the interface Snapshot stores must implement.
type SnapshotStore interface {
	raft.SnapshotStore

	// FullNeeded returns true if a full snapshot is needed.
	FullNeeded() (bool, error)

	// SetFullNeeded explicitly sets that a full snapshot is needed.
	SetFullNeeded() error

	// Stats returns stats about the Snapshot Store.
	Stats() (map[string]interface{}, error)
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

	restoreChunkSize int64
	restorePath      string
	restoreDoneCh    chan struct{}

	raft   *raft.Raft // The consensus mechanism.
	ln     Listener
	raftTn *NodeTransport
	raftID string    // Node ID.
	dbConf *DBConfig // SQLite database config.
	dbPath string    // Path to underlying SQLite file.
	db     *sql.DB   // The underlying SQLite store.

	queryTxMu sync.RWMutex

	dbAppliedIndexMu     sync.RWMutex
	dbAppliedIndex       uint64
	appliedIdxUpdateDone chan struct{}

	dechunkManager    *chunking.DechunkerManager
	loadsInProgressMu sync.Mutex
	loadsInProgress   map[string]struct{}

	// Channels that must be closed for the Store to be considered ready.
	readyChans             []<-chan struct{}
	numClosedReadyChannels int
	readyChansMu           sync.Mutex

	// Channels for triggering and listening to snapshots
	snapshotTClose chan struct{}
	snapshotTDone  chan struct{}
	snapshotTChan  chan struct{}

	// Latest log entry index actually reflected by the FSM. Due to Raft code
	// this value is not updated after a Snapshot-restore.
	fsmIndex   uint64
	fsmIndexMu sync.RWMutex

	reqMarshaller *command.RequestMarshaler // Request marshaler for writing to log.
	raftLog       raft.LogStore             // Persistent log store.
	raftStable    raft.StableStore          // Persistent k-v store.
	boltStore     *rlog.Log                 // Physical store.
	snapshotStore SnapshotStore             // Snapshot store.

	// Raft changes observer
	leaderObserversMu sync.RWMutex
	leaderObservers   []chan<- struct{}
	observerClose     chan struct{}
	observerDone      chan struct{}
	observerChan      chan raft.Observation
	observer          *raft.Observer

	firstIdxOnOpen       uint64    // First index on log when Store opens.
	lastIdxOnOpen        uint64    // Last index on log when Store opens.
	lastCommandIdxOnOpen uint64    // Last command index before applied index when Store opens.
	lastAppliedIdxOnOpen uint64    // Last applied index on log when Store opens.
	firstLogAppliedT     time.Time // Time first log is applied
	appliedOnOpen        uint64    // Number of logs applied at open.
	openT                time.Time // Timestamp when Store opens.

	logger *log.Logger

	notifyMu        sync.Mutex
	BootstrapExpect int
	bootstrapped    bool
	notifyingNodes  map[string]*Server

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
		ln:               ln,
		raftDir:          c.Dir,
		peersPath:        filepath.Join(c.Dir, peersPath),
		peersInfoPath:    filepath.Join(c.Dir, peersInfoPath),
		restoreChunkSize: defaultChunkSize,
		restoreDoneCh:    make(chan struct{}),
		loadsInProgress:  make(map[string]struct{}),
		raftID:           c.ID,
		dbConf:           c.DBConf,
		dbPath:           dbPath,
		leaderObservers:  make([]chan<- struct{}, 0),
		reqMarshaller:    command.NewRequestMarshaler(),
		logger:           logger,
		notifyingNodes:   make(map[string]*Server),
		ApplyTimeout:     applyTimeout,
	}
}

// SetRestorePath sets the path to a file containing a copy of a
// SQLite database. This database will be loaded if and when the
// node becomes the Leader for the first time only. The Store will
// also delete the file when it's finished with it.
//
// This function should only be called before the Store is opened
// and setting the restore path means the Store will not report
// itself as ready until a restore has been attempted.
func (s *Store) SetRestorePath(path string) error {
	if s.open {
		return ErrOpen
	}

	if !sql.IsValidSQLiteFile(path) {
		return fmt.Errorf("file %s is not a valid SQLite file", path)
	}
	if sql.IsWALModeEnabledSQLiteFile(path) {
		return fmt.Errorf("file %s is in WAL mode - convert to DELETE mode", path)
	}

	s.RegisterReadyChannel(s.restoreDoneCh)
	s.restorePath = path
	return nil
}

// SetRestoreChunkSize sets the chunk size to use when restoring a database.
// If not set, the default chunk size is used.
func (s *Store) SetRestoreChunkSize(size int64) {
	s.restoreChunkSize = size
}

// Open opens the Store.
func (s *Store) Open() (retErr error) {
	defer func() {
		if retErr == nil {
			s.open = true
		}
	}()

	if s.open {
		return ErrOpen
	}

	s.openT = time.Now()
	s.logger.Printf("opening store with node ID %s, listening on %s", s.raftID, s.ln.Addr().String())

	// Create all the required Raft directories.
	s.logger.Printf("ensuring data directory exists at %s", s.raftDir)
	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.peersPath), 0755); err != nil {
		return err
	}
	decMgmr, err := chunking.NewDechunkerManager(filepath.Dir(s.dbPath))
	if err != nil {
		return err
	}
	s.dechunkManager = decMgmr

	// Create the database directory, if it doesn't already exist.
	parentDBDir := filepath.Dir(s.dbPath)
	if !dirExists(parentDBDir) {
		s.logger.Printf("creating directory for database at %s", parentDBDir)
		err := os.MkdirAll(parentDBDir, 0755)
		if err != nil {
			return err
		}
	}

	// Create Raft-compatible network layer.
	nt := raft.NewNetworkTransport(NewTransport(s.ln), connectionPoolCount, connectionTimeout, nil)
	s.raftTn = NewNodeTransport(nt)

	// Don't allow control over trailing logs directly, just implement a policy.
	s.numTrailingLogs = uint64(float64(s.SnapshotThreshold) * trailingScale)

	config := s.raftConfig()
	config.LocalID = raft.ServerID(s.raftID)

	// Upgrade any pre-existing snapshots.
	oldSnapshotDir := filepath.Join(s.raftDir, "snapshots")
	snapshotDir := filepath.Join(s.raftDir, "rsnapshots")
	if err := snapshot.Upgrade(oldSnapshotDir, snapshotDir, s.logger); err != nil {
		return fmt.Errorf("failed to upgrade snapshots: %s", err)
	}

	// Create store for the Snapshots.
	snapshotStore, err := snapshot.NewStore(filepath.Join(snapshotDir))
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %s", err)
	}
	s.snapshotStore = snapshotStore
	snaps, err := s.snapshotStore.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %s", err)
	}
	s.logger.Printf("%d preexisting snapshots present", len(snaps))

	// Create the Raft log store and stable store.
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
		if err = RecoverNode(s.raftDir, s.logger, s.raftLog, s.boltStore, s.snapshotStore, s.raftTn, config); err != nil {
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
	s.logger.Printf("first log index: %d, last log index: %d, last applied index: %d, last command log index: %d:",
		s.firstIdxOnOpen, s.lastIdxOnOpen, s.lastAppliedIdxOnOpen, s.lastCommandIdxOnOpen)

	s.db, err = createOnDisk(nil, s.dbPath, s.dbConf.FKConstraints, true)
	if err != nil {
		return fmt.Errorf("failed to create on-disk database: %s", err)
	}
	s.logger.Printf("created on-disk database at open")

	// Clean up any files from aborted restores.
	files, err := filepath.Glob(filepath.Join(s.db.Path(), restoreScratchPattern))
	if err != nil {
		return fmt.Errorf("failed to locate temporary restore files: %s", err.Error())
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("failed to remove temporary restore file %s: %s", f, err.Error())
		}
	}

	// Instantiate the Raft system.
	ra, err := raft.NewRaft(config, NewFSM(s), s.raftLog, s.raftStable, s.snapshotStore, s.raftTn)
	if err != nil {
		return fmt.Errorf("creating the raft system failed: %s", err)
	}
	s.raft = ra

	// Open the observer channels.
	s.observerChan = make(chan raft.Observation, observerChanLen)
	s.observer = raft.NewObserver(s.observerChan, false, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		return isLeaderChange || isFailedHeartBeat
	})

	// Register and listen for leader changes.
	s.raft.RegisterObserver(s.observer)
	s.observerClose, s.observerDone = s.observe()

	// Setup user-triggered snapshotting.
	s.snapshotTChan, s.snapshotTClose, s.snapshotTDone = s.runSnapshotting()

	// Periodically update the applied index for faster startup.
	s.appliedIdxUpdateDone = s.updateAppliedIndex()

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
	s.logger.Printf("closing store with node ID %s, listening on %s", s.raftID, s.ln.Addr().String())

	s.dechunkManager.Close()

	close(s.appliedIdxUpdateDone)
	close(s.observerClose)
	<-s.observerDone

	close(s.snapshotTClose)
	<-s.snapshotTDone

	f := s.raft.Shutdown()
	if wait {
		if f.Error() != nil {
			return f.Error()
		}
	}
	if err := s.raftTn.Close(); err != nil {
		return err
	}

	// Only shutdown Bolt and SQLite when Raft is done.
	if err := s.db.Close(); err != nil {
		return err
	}
	if err := s.boltStore.Close(); err != nil {
		return err
	}

	// Open-and-close again to remove the -wal file. This is not
	// strictly necessary, since any on-disk database files will be removed when
	// rqlite next starts, but it leaves the directory containing the database
	// file in a cleaner state.
	walDB, err := sql.Open(s.dbPath, s.dbConf.FKConstraints, true)
	if err != nil {
		return err
	}
	if err := walDB.Close(); err != nil {
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
	if !s.open {
		return ""
	}
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
	addr, _ := s.raft.LeaderWithID()
	return string(addr), nil
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (s *Store) LeaderID() (string, error) {
	if !s.open {
		return "", nil
	}
	_, id := s.raft.LeaderWithID()
	return string(id), nil
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Store) LeaderWithID() (string, string) {
	if !s.open {
		return "", ""
	}
	addr, id := s.raft.LeaderWithID()
	return string(addr), string(id)
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

// WaitForRemoval blocks until a node with the given ID is removed from the
// cluster or the timeout expires.
func (s *Store) WaitForRemoval(id string, timeout time.Duration) error {
	check := func() bool {
		nodes, err := s.Nodes()
		if err == nil && !Servers(nodes).Contains(id) {
			return true
		}
		return false
	}

	// try the fast path
	if check() {
		return nil
	}

	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for {
		select {
		case <-tck.C:
			if check() {
				return nil
			}
		case <-tmr.C:
			return ErrWaitForRemovalTimeout
		}
	}
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *Store) WaitForLeader(timeout time.Duration) (string, error) {
	var err error
	var leaderAddr string

	check := func() bool {
		leaderAddr, err = s.LeaderAddr()
		if err == nil && leaderAddr != "" {
			return true
		}
		return false
	}

	// try the fast path
	if check() {
		return leaderAddr, nil
	}

	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for {
		select {
		case <-tck.C:
			if check() {
				return leaderAddr, nil
			}
		case <-tmr.C:
			if err != nil {
				s.logger.Printf("timed out waiting for leader, last error: %s", err.Error())
			}
			return "", ErrWaitForLeaderTimeout
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
		stats.Add(numDBStatsErrors, 1)
		s.logger.Printf("failed to get database stats: %s", err.Error())
	}

	nodes, err := s.Nodes()
	if err != nil {
		return nil, err
	}
	leaderAddr, leaderID := s.LeaderWithID()

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

	snapsStats, err := s.snapshotStore.Stats()
	if err != nil {
		return nil, err
	}

	lAppliedIdx, err := s.boltStore.GetAppliedIndex()
	if err != nil {
		return nil, err
	}
	status := map[string]interface{}{
		"open":               s.open,
		"node_id":            s.raftID,
		"raft":               raftStats,
		"fsm_index":          fsmIdx,
		"db_applied_index":   dbAppliedIdx,
		"last_applied_index": lAppliedIdx,
		"addr":               s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    leaderAddr,
		},
		"ready": s.Ready(),
		"observer": map[string]uint64{
			"observed": s.observer.GetNumObserved(),
			"dropped":  s.observer.GetNumDropped(),
		},
		"apply_timeout":          s.ApplyTimeout.String(),
		"heartbeat_timeout":      s.HeartbeatTimeout.String(),
		"election_timeout":       s.ElectionTimeout.String(),
		"snapshot_store":         snapsStats,
		"snapshot_threshold":     s.SnapshotThreshold,
		"snapshot_interval":      s.SnapshotInterval.String(),
		"reap_timeout":           s.ReapTimeout.String(),
		"reap_read_only_timeout": s.ReapReadOnlyTimeout.String(),
		"no_freelist_sync":       s.NoFreeListSync,
		"trailing_logs":          s.numTrailingLogs,
		"request_marshaler":      s.reqMarshaller.Stats(),
		"nodes":                  nodes,
		"loads_in_progress":      s.NumLoadsInProgress(),
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
	b, compressed, err := s.tryCompress(ex)
	if err != nil {
		return nil, err
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

		b, compressed, err := s.tryCompress(qr)
		if err != nil {
			return nil, err
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

// Request processes a request that may contain both Executes and Queries.
func (s *Store) Request(eqr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, error) {
	if !s.open {
		return nil, ErrNotOpen
	}

	if !s.RequiresLeader(eqr) {
		if eqr.Level == command.QueryRequest_QUERY_REQUEST_LEVEL_NONE && eqr.Freshness > 0 &&
			time.Since(s.raft.LastContact()).Nanoseconds() > eqr.Freshness {
			return nil, ErrStaleRead
		}
		if eqr.Request.Transaction {
			// Transaction requested during query, but not going through consensus. This means
			// we need to block any database serialization during the query.
			s.queryTxMu.RLock()
			defer s.queryTxMu.RUnlock()
		}
		return s.db.Request(eqr.Request, eqr.Timings)
	}

	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	if !s.Ready() {
		return nil, ErrNotReady
	}

	b, compressed, err := s.tryCompress(eqr)
	if err != nil {
		return nil, err
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_EXECUTE_QUERY,
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
	r := af.Response().(*fsmExecuteQueryResponse)
	return r.results, r.error
}

// Backup writes a consistent snapshot of the underlying database to dst.
func (s *Store) Backup(br *command.BackupRequest, dst io.Writer) (retErr error) {
	if !s.open {
		return ErrNotOpen
	}

	if br.Vacuum && br.Format != command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
		return ErrInvalidBackupFormat
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
		f, err := os.CreateTemp("", "rqlite-backup-*")
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		defer os.Remove(f.Name())

		if err := s.db.Backup(f.Name(), br.Vacuum); err != nil {
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

// LoadFromReader reads data from r chunk-by-chunk, and loads it into the
// database.
func (s *Store) LoadFromReader(r io.Reader, chunkSize int64) error {
	if !s.open {
		return ErrNotOpen
	}

	if !s.Ready() {
		return ErrNotReady
	}

	return s.loadFromReader(r, chunkSize)
}

// loadFromReader reads data from r chunk-by-chunk, and loads it into the
// database. It is for internal use only. It does not check for readiness.
func (s *Store) loadFromReader(r io.Reader, chunkSize int64) error {
	chunker := chunking.NewChunker(r, chunkSize)
	for {
		chunk, err := chunker.Next()
		if err != nil {
			return err
		}
		if err := s.loadChunk(chunk); err != nil {
			return err
		}
		if chunk.IsLast {
			break
		}
	}
	return nil
}

// LoadChunk loads a chunk of data into the database, sending the request
// through the Raft log.
func (s *Store) LoadChunk(lcr *command.LoadChunkRequest) error {
	if !s.open {
		return ErrNotOpen
	}

	if !s.Ready() {
		return ErrNotReady
	}

	return s.loadChunk(lcr)
}

// loadChunk loads a chunk of data into the database, and is for internal use
// only. It does not check for readiness.
func (s *Store) loadChunk(lcr *command.LoadChunkRequest) error {
	b, err := command.MarshalLoadChunkRequest(lcr)
	if err != nil {
		return err
	}

	c := &command.Command{
		Type:       command.Command_COMMAND_TYPE_LOAD_CHUNK,
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
		return af.Error()
	}

	// If it's the last chunk, send a NOOP through to deal with
	// strange issues in the Raft code with Trailing Logs set
	// to zero.
	if lcr.IsLast || lcr.Abort {
		if err := s.Noop("last-chunk-trailing-logs"); err != nil {
			return err
		}

	}

	s.dbAppliedIndexMu.Lock()
	s.dbAppliedIndex = af.Index()
	s.dbAppliedIndexMu.Unlock()
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

	if err := s.load(lr); err != nil {
		return err
	}
	stats.Add(numLoads, 1)
	return nil
}

// load loads an entire SQLite file into the database, and is for internal use
// only. It does not check for readiness, and does not update statistics.
func (s *Store) load(lr *command.LoadRequest) error {
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
	s.logger.Printf("node loaded in %s (%d bytes)", time.Since(startT), len(b))

	return nil
}

// ReadFrom reads data from r, and loads it into the database, bypassing Raft consensus.
// Once the data is loaded, a snapshot is triggered, which then results in a system as
// if the data had been loaded through Raft consensus.
func (s *Store) ReadFrom(r io.Reader) (int64, error) {
	// Check the constraints.
	if s.raft.State() != raft.Leader {
		return 0, ErrNotLeader
	}
	nodes, err := s.Nodes()
	if err != nil {
		return 0, err
	}
	if len(nodes) != 1 {
		return 0, ErrNotSingleNode
	}

	// Write the data to a temporary file..
	f, err := os.CreateTemp("", "rqlite-bypass-*")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	defer os.Remove(f.Name())
	n, err := io.Copy(f, r)
	if err != nil {
		return n, err
	}
	f.Close()

	// Close the database, remove the database file, and then rename the temporary
	if err := s.db.Close(); err != nil {
		return n, err
	}
	if err := sql.RemoveFiles(s.dbPath); err != nil {
		return n, err
	}
	if err := os.Rename(f.Name(), s.dbPath); err != nil {
		return n, err
	}
	db, err := sql.Open(s.dbPath, s.dbConf.FKConstraints, true)
	if err != nil {
		return n, err
	}
	s.db = db

	// Raft won't snapshot unless there is at least one unsnappshotted log entry.
	if err := s.Noop("bypass"); err != nil {
		return n, err
	}
	if err := s.snapshotStore.SetFullNeeded(); err != nil {
		return n, err
	}
	if err := s.Snapshot(); err != nil {
		return n, err
	}
	stats.Add(numLoadsBypass, 1)
	return n, nil
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

	// Confirm that this node can resolve the remote address. This can happen due
	// to incomplete DNS records across the underlying infrastructure. If it can't
	// then don't consider this Notify attempt successful -- so the notifying node
	// will presumably try again.
	if addr, err := resolvableAddress(nr.Address); err != nil {
		return fmt.Errorf("failed to resolve %s: %w", addr, err)
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

	// Confirm that this node can resolve the remote address. This can happen due
	// to incomplete DNS records across the underlying infrastructure. If it can't
	// then don't consider this join attempt successful -- so the joining node
	// will presumably try again.
	if addr, err := resolvableAddress(addr); err != nil {
		return fmt.Errorf("failed to resolve %s: %w", addr, err)
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

// RequiresLeader returns whether the given ExecuteQueryRequest must be
// processed on the cluster Leader.
func (s *Store) RequiresLeader(eqr *command.ExecuteQueryRequest) bool {
	if eqr.Level != command.QueryRequest_QUERY_REQUEST_LEVEL_NONE {
		return true
	}

	for _, stmt := range eqr.Request.Statements {
		sql := stmt.Sql
		if sql == "" {
			continue
		}
		ro, err := s.db.StmtReadOnly(sql)
		if !ro || err != nil {
			return true
		}
	}
	return false
}

// setLogInfo records some key indexes about the log.
func (s *Store) setLogInfo() error {
	var err error
	s.firstIdxOnOpen, err = s.boltStore.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get last index: %s", err)
	}
	s.lastAppliedIdxOnOpen, err = s.boltStore.GetAppliedIndex()
	if err != nil {
		return fmt.Errorf("failed to get last applied index: %s", err)
	}
	s.lastIdxOnOpen, err = s.boltStore.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to get last index: %s", err)
	}
	s.lastCommandIdxOnOpen, err = s.boltStore.LastCommandIndex(s.firstIdxOnOpen, s.lastAppliedIdxOnOpen)
	if err != nil {
		return fmt.Errorf("failed to get last command index: %s", err)
	}
	return nil
}

// remove removes the node, with the given ID, from the cluster.
func (s *Store) remove(id string) error {
	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if f.Error() != nil && f.Error() == raft.ErrNotLeader {
		return ErrNotLeader
	}
	return f.Error()
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

func (s *Store) updateAppliedIndex() chan struct{} {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(appliedIndexUpdateInterval)
		defer ticker.Stop()
		var idx uint64
		for {
			select {
			case <-ticker.C:
				newIdx := s.raft.AppliedIndex()
				if newIdx == idx {
					continue
				}
				idx = newIdx
				if err := s.boltStore.SetAppliedIndex(idx); err != nil {
					s.logger.Printf("failed to set applied index: %s", err.Error())
				}
			case <-done:
				return
			}
		}
	}()
	return done
}

type fsmExecuteResponse struct {
	results []*command.ExecuteResult
	error   error
}

type fsmQueryResponse struct {
	rows  []*command.QueryRows
	error error
}

type fsmExecuteQueryResponse struct {
	results []*command.ExecuteQueryResponse
	error   error
}

type fsmGenericResponse struct {
	error error
}

// fsmApply applies a Raft log entry to the database.
func (s *Store) fsmApply(l *raft.Log) (e interface{}) {
	defer func() {
		s.fsmIndexMu.Lock()
		defer s.fsmIndexMu.Unlock()
		s.fsmIndex = l.Index

		if l.Index <= s.lastCommandIdxOnOpen {
			// In here means at least one command entry was in the log when the Store
			// opened.
			s.appliedOnOpen++
			if l.Index == s.lastCommandIdxOnOpen {
				s.logger.Printf("%d confirmed committed log entries applied in %s, took %s since open",
					s.appliedOnOpen, time.Since(s.firstLogAppliedT), time.Since(s.openT))
			}
		}
	}()

	if s.firstLogAppliedT.IsZero() {
		s.firstLogAppliedT = time.Now()
		s.logger.Printf("first log applied since node start, log at index %d", l.Index)
	}

	snapshotNeeded := false
	cmd, r := applyCommand(l.Data, &s.db, s.dechunkManager)
	switch cmd.Type {
	case command.Command_COMMAND_TYPE_NOOP:
		s.numNoops++
	case command.Command_COMMAND_TYPE_LOAD:
		snapshotNeeded = true
	case command.Command_COMMAND_TYPE_LOAD_CHUNK:
		var lcr command.LoadChunkRequest
		if err := command.UnmarshalLoadChunkRequest(cmd.SubCommand, &lcr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal load-chunk subcommand: %s", err.Error()))
		}
		snapshotNeeded = lcr.IsLast
		func() {
			s.loadsInProgressMu.Lock()
			defer s.loadsInProgressMu.Unlock()
			if lcr.IsLast || lcr.Abort {
				delete(s.loadsInProgress, lcr.StreamId)
			} else {
				s.loadsInProgress[lcr.StreamId] = struct{}{}
			}
		}()
	}

	if snapshotNeeded {
		if err := s.snapshotStore.SetFullNeeded(); err != nil {
			return &fsmGenericResponse{error: fmt.Errorf("failed to SetFullNeeded post load: %s", err)}
		}
		s.logger.Printf("last chunk loaded, forcing snapshot of database")
		s.snapshotTChan <- struct{}{}
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

// fsmSnapshot returns a snapshot of the database.
//
// The system must ensure that no transaction is taking place during this call.
// Hashicorp Raft guarantees that this function will not be called concurrently
// with Apply, as it states Apply() and Snapshot() are always called from the same
// thread. This means there is no need to synchronize this function with Execute().
// However, queries that involve a transaction must be blocked.
//
// http://sqlite.org/howtocorrupt.html states it is safe to copy or serialize the
// database as long as no writes to the database are in progress.
func (s *Store) fsmSnapshot() (raft.FSMSnapshot, error) {
	startT := time.Now()

	if s.NumLoadsInProgress() > 0 {
		// There are loads in progress. Now, the next thing that has to be true
		// is that the last log entry must be a load-chunk command, and it must
		// indicate an ongoing load. If that is not the case, then some other
		// request has been sent to this systemg, and all load operations are invalid.
		lastIdx, err := s.boltStore.LastIndex()
		if err != nil {
			return nil, err
		}
		lastLog := &raft.Log{}
		if err := s.boltStore.GetLog(lastIdx, lastLog); err != nil {
			return nil, err
		}
		var c command.Command
		if err := command.Unmarshal(lastLog.Data, &c); err != nil {
			panic(fmt.Sprintf("failed to unmarshal last log entry: %s", err.Error()))
		}
		if c.Type == command.Command_COMMAND_TYPE_LOAD_CHUNK {
			var lcr command.LoadChunkRequest
			if err := command.UnmarshalLoadChunkRequest(c.SubCommand, &lcr); err != nil {
				panic(fmt.Sprintf("failed to unmarshal load-chunk subcommand: %s", err.Error()))
			}
			if !lcr.IsLast && !lcr.Abort {
				return nil, ErrLoadInProgress
			}
		}
	}

	fullNeeded, err := s.snapshotStore.FullNeeded()
	if err != nil {
		return nil, err
	}
	fPLog := fullPretty(fullNeeded)
	s.logger.Printf("initiating %s snapshot on node ID %s", fPLog, s.raftID)
	defer func() {
		s.numSnapshotsMu.Lock()
		defer s.numSnapshotsMu.Unlock()
		s.numSnapshots++
	}()

	s.queryTxMu.Lock()
	defer s.queryTxMu.Unlock()

	var fsmSnapshot raft.FSMSnapshot
	if fullNeeded {
		if err := s.db.Checkpoint(); err != nil {
			return nil, err
		}
		dbFD, err := os.Open(s.db.Path())
		if err != nil {
			return nil, err
		}
		fsmSnapshot = snapshot.NewSnapshot(dbFD)
		stats.Add(numSnapshotsFull, 1)
	} else {
		compactedBuf := bytes.NewBuffer(nil)
		var err error
		if pathExistsWithData(s.db.WALPath()) {
			walFD, err := os.Open(s.db.WALPath())
			if err != nil {
				return nil, err
			}
			defer walFD.Close() // Make sure it closes.

			scanner, err := wal.NewFastCompactingScanner(walFD)
			if err != nil {
				return nil, err
			}
			walWr, err := wal.NewWriter(scanner)
			if err != nil {
				return nil, err
			}
			if _, err := walWr.WriteTo(compactedBuf); err != nil {
				return nil, err
			}
			walFD.Close() // We need it closed for the next step.

			walSz, err := fileSize(s.db.WALPath())
			if err != nil {
				return nil, err
			}
			stats.Get(snapshotWALSize).(*expvar.Int).Set(int64(compactedBuf.Len()))
			stats.Get(snapshotPrecompactWALSize).(*expvar.Int).Set(walSz)
			s.logger.Printf("%s snapshot is %d bytes (WAL=%d bytes) on node ID %s", fPLog, compactedBuf.Len(),
				walSz, s.raftID)
			if err := s.db.Checkpoint(); err != nil {
				return nil, err
			}
		}
		fsmSnapshot = snapshot.NewSnapshot(io.NopCloser(compactedBuf))
		if err != nil {
			return nil, err
		}
		stats.Add(numSnapshotsIncremental, 1)
	}

	stats.Add(numSnapshots, 1)
	dur := time.Since(startT)
	stats.Get(snapshotCreateDuration).(*expvar.Int).Set(dur.Milliseconds())
	s.logger.Printf("%s snapshot created in %s on node ID %s", fPLog, dur, s.raftID)
	return &FSMSnapshot{
		FSMSnapshot: fsmSnapshot,
		logger:      s.logger,
	}, nil
}

// fsmRestore restores the node to a previous state. The Hashicorp docs state this
// will not be called concurrently with Apply(), so synchronization with Execute()
// is not necessary.
func (s *Store) fsmRestore(rc io.ReadCloser) error {
	s.logger.Printf("initiating node restore on node ID %s", s.raftID)
	startT := time.Now()

	// Create a scatch file to write the restore data to it.
	tmpFile, err := os.CreateTemp(filepath.Dir(s.db.Path()), restoreScratchPattern)
	if err != nil {
		return fmt.Errorf("error creating temporary file for restore operation: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Copy it from the reader to the temporary file.
	_, err = io.Copy(tmpFile, rc)
	if err != nil {
		return fmt.Errorf("error copying restore data: %v", err)
	}
	if tmpFile.Close(); err != nil {
		return fmt.Errorf("error creating temporary file for restore operation: %v", err)
	}

	// Must wipe out all pre-existing state if being asked to do a restore, and put
	// the new database in place.
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close pre-restore database: %s", err)
	}
	if err := sql.RemoveFiles(s.db.Path()); err != nil {
		return fmt.Errorf("failed to remove pre-restore database files: %s", err)
	}
	if err := os.Rename(tmpFile.Name(), s.db.Path()); err != nil {
		return fmt.Errorf("failed to rename restored database: %s", err)
	}

	var db *sql.DB
	db, err = sql.Open(s.dbPath, s.dbConf.FKConstraints, true)
	if err != nil {
		return fmt.Errorf("open SQLite file during restore: %s", err)
	}
	s.db = db
	s.logger.Printf("successfully opened database at %s due to restore", s.db.Path())

	stats.Add(numRestores, 1)
	s.logger.Printf("node restored in %s", time.Since(startT))
	rc.Close()
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
					s.selfLeaderChange(signal.LeaderID == raft.ServerID(s.raftID))
				}

			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

// Snapshot performs a snapshot, and then truncates the Raft log except for the
// very last log. It does this to work around apparent issues in the log
// compaction of Hashicorp Raft. If trailing logs are set to zero, then
// snapshotting never stops.
func (s *Store) Snapshot() error {
	// reload the config with zero trailing logs.
	cfg := s.raft.ReloadableConfig()
	defer func() {
		// Whatever happens, this is a one-shot attempt to perform a snapshot
		cfg.TrailingLogs = s.numTrailingLogs
		if err := s.raft.ReloadConfig(cfg); err != nil {
			s.logger.Printf("failed to reload Raft config: %s", err.Error())
		}
	}()
	cfg.TrailingLogs = 1
	if err := s.raft.ReloadConfig(cfg); err != nil {
		return fmt.Errorf("failed to reload Raft config: %s", err.Error())
	}
	if err := s.raft.Snapshot().Error(); err != nil {
		if strings.Contains(err.Error(), ErrLoadInProgress.Error()) {
			return ErrLoadInProgress
		}
		return err
	}
	stats.Add(numSnapshots, 1)
	return nil
}

// runSnapshotting runs the user-requested snapshotting, and returns the
// trigger channel, the close channel, and the done channel. Unless Raft
// triggered Snapshotting the log is also compacted after each snapshot.
func (s *Store) runSnapshotting() (triggerT, closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})
	triggerT = make(chan struct{})
	go func() {
		defer close(doneCh)
		for {
			select {
			case <-triggerT:
				if err := s.Snapshot(); err != nil {
					s.logger.Printf("failed to snapshot: %s", err.Error())
				}
			case <-closeCh:
				return
			}
		}
	}()
	return triggerT, closeCh, doneCh
}

// selfLeaderChange is called when this node detects that its leadership
// status has changed.
func (s *Store) selfLeaderChange(leader bool) {
	if s.restorePath != "" {
		defer func() {
			// Whatever happens, this is a one-shot attempt to perform a restore
			err := os.Remove(s.restorePath)
			if err != nil {
				s.logger.Printf("failed to remove restore path after restore %s: %s",
					s.restorePath, err.Error())
			}
			s.restorePath = ""
			close(s.restoreDoneCh)
		}()

		if !leader {
			s.logger.Printf("different node became leader, not performing auto-restore")
			stats.Add(numAutoRestoresSkipped, 1)
		} else {
			s.logger.Printf("this node is now leader, auto-restoring from %s", s.restorePath)
			if err := s.installRestore(); err != nil {
				s.logger.Printf("failed to auto-restore from %s: %s", s.restorePath, err.Error())
				stats.Add(numAutoRestoresFailed, 1)
				return
			}
			stats.Add(numAutoRestores, 1)
			s.logger.Printf("node auto-restored successfully from %s", s.restorePath)
		}
	}
}

func (s *Store) installRestore() error {
	f, err := os.Open(s.restorePath)
	if err != nil {
		return err
	}
	defer f.Close()
	return s.loadFromReader(f, s.restoreChunkSize)
}

// logSize returns the size of the Raft log on disk.
func (s *Store) logSize() (int64, error) {
	fi, err := os.Stat(filepath.Join(s.raftDir, raftDBPath))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// NumLoadsInProgress returns the number of loads currently in progress.
func (s *Store) NumLoadsInProgress() int {
	s.loadsInProgressMu.Lock()
	defer s.loadsInProgressMu.Unlock()
	return len(s.loadsInProgress)
}

// tryCompress attempts to compress the given command. If the command is
// successfully compressed, the compressed byte slice is returned, along with
// a boolean true. If the command cannot be compressed, the uncompressed byte
// slice is returned, along with a boolean false. The stats are updated
// accordingly.
func (s *Store) tryCompress(rq command.Requester) ([]byte, bool, error) {
	b, compressed, err := s.reqMarshaller.Marshal(rq)
	if err != nil {
		return nil, false, err
	}
	if compressed {
		stats.Add(numCompressedCommands, 1)
	} else {
		stats.Add(numUncompressedCommands, 1)
	}
	return b, compressed, nil
}

// RecoverNode is used to manually force a new configuration, in the event that
// quorum cannot be restored. This borrows heavily from RecoverCluster functionality
// of the Hashicorp Raft library, but has been customized for rqlite use.
func RecoverNode(dataDir string, logger *log.Logger, logs raft.LogStore, stable *rlog.Log,
	snaps raft.SnapshotStore, tn raft.Transport, conf raft.Configuration) error {
	logPrefix := logger.Prefix()
	logger.SetPrefix(fmt.Sprintf("%s[recovery] ", logPrefix))
	defer logger.SetPrefix(logPrefix)

	// Sanity check the Raft peer configuration.
	if err := checkRaftConfiguration(conf); err != nil {
		return err
	}

	// Get a path to a temporary file to use for a temporary database.
	tmpDBPath := filepath.Join(dataDir, "recovery.db")
	defer os.Remove(tmpDBPath)

	// Attempt to restore any latest snapshot.
	var (
		snapshotIndex uint64
		snapshotTerm  uint64
	)

	snapshots, err := snaps.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %s", err)
	}
	logger.Printf("recovery detected %d snapshots", len(snapshots))
	if len(snapshots) > 0 {
		if err := func() error {
			snapID := snapshots[0].ID
			_, rc, err := snaps.Open(snapID)
			if err != nil {
				return fmt.Errorf("failed to open snapshot %s: %s", snapID, err)
			}
			defer rc.Close()
			_, err = copyFromReaderToFile(tmpDBPath, rc)
			if err != nil {
				return fmt.Errorf("failed to copy snapshot %s to temporary database: %s", snapID, err)
			}
			snapshotIndex = snapshots[0].Index
			snapshotTerm = snapshots[0].Term
			return nil
		}(); err != nil {
			return err
		}
	}

	// Now, open the database so we can replay any outstanding Raft log entries.
	db, err := sql.Open(tmpDBPath, false, true)
	if err != nil {
		return fmt.Errorf("failed to open temporary database: %s", err)
	}
	defer db.Close()

	// Need a dechunker manager to handle any chunked load requests.
	decMgmr, err := chunking.NewDechunkerManager(dataDir)
	if err != nil {
		return fmt.Errorf("failed to create dechunker manager: %s", err.Error())
	}

	// The snapshot information is the best known end point for the data
	// until we play back the Raft log entries.
	lastIndex := snapshotIndex
	lastTerm := snapshotTerm

	// Apply any Raft log entries past the snapshot.
	lastLogIndex, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to find last log: %v", err)
	}
	logger.Printf("last index is %d, last index written to log is %d", lastIndex, lastLogIndex)

	for index := snapshotIndex + 1; index <= lastLogIndex; index++ {
		var entry raft.Log
		if err = logs.GetLog(index, &entry); err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", index, err)
		}
		if entry.Type == raft.LogCommand {
			applyCommand(entry.Data, &db, decMgmr)
		}
		lastIndex = entry.Index
		lastTerm = entry.Term
	}

	// Create a new snapshot, placing the configuration in as if it was
	// committed at index 1.
	if err := db.Checkpoint(); err != nil {
		return fmt.Errorf("failed to checkpoint database: %s", err)
	}
	tmpDBFD, err := os.Open(tmpDBPath)
	if err != nil {
		return fmt.Errorf("failed to open temporary database file: %s", err)
	}
	fsmSnapshot := snapshot.NewSnapshot(tmpDBFD) // tmpDBPath contains full state now.
	sink, err := snaps.Create(1, lastIndex, lastTerm, conf, 1, tn)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	if err = fsmSnapshot.Persist(sink); err != nil {
		return fmt.Errorf("failed to persist snapshot: %v", err)
	}
	if err = sink.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}
	logger.Printf("recovery snapshot created successfully using %s", tmpDBPath)

	// Compact the log so that we don't get bad interference from any
	// configuration change log entries that might be there.
	firstLogIndex, err := logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}
	if err := logs.DeleteRange(firstLogIndex, lastLogIndex); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}

	// Erase record of previous updating of Applied Index too.
	if err := stable.SetAppliedIndex(0); err != nil {
		return fmt.Errorf("failed to zero applied index: %v", err)
	}
	return nil
}

func applyCommand(data []byte, pDB **sql.DB, decMgmr *chunking.DechunkerManager) (*command.Command, interface{}) {
	c := &command.Command{}
	db := *pDB

	if err := command.Unmarshal(data, c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
	}

	switch c.Type {
	case command.Command_COMMAND_TYPE_QUERY:
		var qr command.QueryRequest
		if err := command.UnmarshalSubCommand(c, &qr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		r, err := db.Query(qr.Request, qr.Timings)
		return c, &fsmQueryResponse{rows: r, error: err}
	case command.Command_COMMAND_TYPE_EXECUTE:
		var er command.ExecuteRequest
		if err := command.UnmarshalSubCommand(c, &er); err != nil {
			panic(fmt.Sprintf("failed to unmarshal execute subcommand: %s", err.Error()))
		}
		r, err := db.Execute(er.Request, er.Timings)
		return c, &fsmExecuteResponse{results: r, error: err}
	case command.Command_COMMAND_TYPE_EXECUTE_QUERY:
		var eqr command.ExecuteQueryRequest
		if err := command.UnmarshalSubCommand(c, &eqr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal execute-query subcommand: %s", err.Error()))
		}
		r, err := db.Request(eqr.Request, eqr.Timings)
		return c, &fsmExecuteQueryResponse{results: r, error: err}
	case command.Command_COMMAND_TYPE_LOAD:
		var lr command.LoadRequest
		if err := command.UnmarshalLoadRequest(c.SubCommand, &lr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal load subcommand: %s", err.Error()))
		}

		// Swap the underlying database to the new one.
		if err := db.Close(); err != nil {
			return c, &fsmGenericResponse{error: fmt.Errorf("failed to close post-load database: %s", err)}
		}
		if err := sql.RemoveFiles(db.Path()); err != nil {
			return c, &fsmGenericResponse{error: fmt.Errorf("failed to remove existing database files: %s", err)}
		}

		newDB, err := createOnDisk(lr.Data, db.Path(), db.FKEnabled(), db.WALEnabled())
		if err != nil {
			return c, &fsmGenericResponse{error: fmt.Errorf("failed to create on-disk database: %s", err)}
		}

		*pDB = newDB
		return c, &fsmGenericResponse{}
	case command.Command_COMMAND_TYPE_LOAD_CHUNK:
		var lcr command.LoadChunkRequest
		if err := command.UnmarshalLoadChunkRequest(c.SubCommand, &lcr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal load-chunk subcommand: %s", err.Error()))
		}

		dec, err := decMgmr.Get(lcr.StreamId)
		if err != nil {
			return c, &fsmGenericResponse{error: fmt.Errorf("failed to get dechunker: %s", err)}
		}
		if lcr.Abort {
			path, err := dec.Close()
			if err != nil {
				return c, &fsmGenericResponse{error: fmt.Errorf("failed to close dechunker: %s", err)}
			}
			decMgmr.Delete(lcr.StreamId)
			defer os.Remove(path)
		} else {
			last, err := dec.WriteChunk(&lcr)
			if err != nil {
				return c, &fsmGenericResponse{error: fmt.Errorf("failed to write chunk: %s", err)}
			}
			if last {
				path, err := dec.Close()
				if err != nil {
					return c, &fsmGenericResponse{error: fmt.Errorf("failed to close dechunker: %s", err)}
				}
				decMgmr.Delete(lcr.StreamId)
				defer os.Remove(path)

				// Close the underlying database before we overwrite it.
				if err := db.Close(); err != nil {
					return c, &fsmGenericResponse{error: fmt.Errorf("failed to close post-load database: %s", err)}
				}
				if err := sql.RemoveFiles(db.Path()); err != nil {
					return c, &fsmGenericResponse{error: fmt.Errorf("failed to remove existing database files: %s", err)}
				}

				if err := os.Rename(path, db.Path()); err != nil {
					return c, &fsmGenericResponse{error: fmt.Errorf("failed to rename temporary database file: %s", err)}
				}
				newDB, err := sql.Open(db.Path(), db.FKEnabled(), db.WALEnabled())
				if err != nil {
					return c, &fsmGenericResponse{error: fmt.Errorf("failed to open new on-disk database: %s", err)}
				}

				// Swap the underlying database to the new one.
				*pDB = newDB
			}
		}
		return c, &fsmGenericResponse{}
	case command.Command_COMMAND_TYPE_NOOP:
		return c, &fsmGenericResponse{}
	default:
		return c, &fsmGenericResponse{error: fmt.Errorf("unhandled command: %v", c.Type)}
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

// createOnDisk opens an on-disk database file at the configured path. If b is
// non-nil, any preexisting file will first be overwritten with those contents.
// Otherwise, any preexisting file will be removed before the database is opened.
func createOnDisk(b []byte, path string, fkConstraints, wal bool) (*sql.DB, error) {
	if err := sql.RemoveFiles(path); err != nil {
		return nil, err
	}
	if b != nil {
		if err := os.WriteFile(path, b, 0660); err != nil {
			return nil, err
		}
	}
	return sql.Open(path, fkConstraints, wal)
}

func copyFromReaderToFile(path string, r io.Reader) (int64, error) {
	fd, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer fd.Close()
	return io.Copy(fd, r)
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

// pathExistsWithData returns true if the given path exists and has data.
func pathExistsWithData(p string) bool {
	if !pathExists(p) {
		return false
	}
	if size, err := fileSize(p); err != nil || size == 0 {
		return false
	}
	return true
}

func dirExists(path string) bool {
	stat, err := os.Stat(path)
	return err == nil && stat.IsDir()
}

func fileSize(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
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

func fullPretty(full bool) string {
	if full {
		return "full"
	}
	return "incremental"
}

func resolvableAddress(addr string) (string, error) {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Just try the given address directly.
		h = addr
	}
	_, err = net.LookupHost(h)
	return h, err
}
