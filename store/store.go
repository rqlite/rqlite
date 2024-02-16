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
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/chunking"
	"github.com/rqlite/rqlite/v8/command/proto"
	sql "github.com/rqlite/rqlite/v8/db"
	"github.com/rqlite/rqlite/v8/db/humanize"
	wal "github.com/rqlite/rqlite/v8/db/wal"
	rlog "github.com/rqlite/rqlite/v8/log"
	"github.com/rqlite/rqlite/v8/progress"
	"github.com/rqlite/rqlite/v8/random"
	"github.com/rqlite/rqlite/v8/snapshot"
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
	snapshotsDirName           = "rsnapshots"
	restoreScratchPattern      = "rqlite-restore-*"
	bootScatchPattern          = "rqlite-boot-*"
	backupScatchPattern        = "rqlite-backup-*"
	vacuumScatchPattern        = "rqlite-vacuum-*"
	raftDBPath                 = "raft.db" // Changing this will break backwards compatibility.
	peersPath                  = "raft/peers.json"
	peersInfoPath              = "raft/peers.info"
	retainSnapshotCount        = 1
	applyTimeout               = 10 * time.Second
	openTimeout                = 120 * time.Second
	sqliteFile                 = "db.sqlite"
	leaderWaitDelay            = 100 * time.Millisecond
	appliedWaitDelay           = 100 * time.Millisecond
	commitEquivalenceDelay     = 50 * time.Millisecond
	appliedIndexUpdateInterval = 5 * time.Second
	connectionPoolCount        = 5
	connectionTimeout          = 10 * time.Second
	raftLogCacheSize           = 512
	trailingScale              = 1.25
	observerChanLen            = 50

	baseVacuumTimeKey = "rqlite_base_vacuum"
	lastVacuumTimeKey = "rqlite_last_vacuum"
)

const (
	numSnapshots                      = "num_snapshots"
	numSnapshotsFailed                = "num_snapshots_failed"
	numUserSnapshots                  = "num_user_snapshots"
	numUserSnapshotsFailed            = "num_user_snapshots_failed"
	numWALSnapshots                   = "num_wal_snapshots"
	numWALSnapshotsFailed             = "num_wal_snapshots_failed"
	numSnapshotsFull                  = "num_snapshots_full"
	numSnapshotsIncremental           = "num_snapshots_incremental"
	numFullCheckpointFailed           = "num_full_checkpoint_failed"
	numWALCheckpointTruncateFailed    = "num_wal_checkpoint_truncate_failed"
	numAutoVacuums                    = "num_auto_vacuums"
	numAutoVacuumsFailed              = "num_auto_vacuums_failed"
	autoVacuumDuration                = "auto_vacuum_duration"
	numBoots                          = "num_boots"
	numBackups                        = "num_backups"
	numLoads                          = "num_loads"
	numRestores                       = "num_restores"
	numRestoresFailed                 = "num_restores_failed"
	numAutoRestores                   = "num_auto_restores"
	numAutoRestoresSkipped            = "num_auto_restores_skipped"
	numAutoRestoresFailed             = "num_auto_restores_failed"
	numRecoveries                     = "num_recoveries"
	numProviderChecks                 = "num_provider_checks"
	numProviderProvides               = "num_provider_provides"
	numProviderProvidesFail           = "num_provider_provides_fail"
	numUncompressedCommands           = "num_uncompressed_commands"
	numCompressedCommands             = "num_compressed_commands"
	numJoins                          = "num_joins"
	numIgnoredJoins                   = "num_ignored_joins"
	numRemovedBeforeJoins             = "num_removed_before_joins"
	numDBStatsErrors                  = "num_db_stats_errors"
	snapshotCreateDuration            = "snapshot_create_duration"
	snapshotCreateChkTruncateDuration = "snapshot_create_chk_truncate_duration"
	snapshotCreateWALCompactDuration  = "snapshot_create_wal_compact_duration"
	snapshotPersistDuration           = "snapshot_persist_duration"
	snapshotPrecompactWALSize         = "snapshot_precompact_wal_size"
	snapshotWALSize                   = "snapshot_wal_size"
	leaderChangesObserved             = "leader_changes_observed"
	leaderChangesDropped              = "leader_changes_dropped"
	failedHeartbeatObserved           = "failed_heartbeat_observed"
	nodesReapedOK                     = "nodes_reaped_ok"
	nodesReapedFailed                 = "nodes_reaped_failed"
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
	stats.Add(numSnapshotsFailed, 0)
	stats.Add(numUserSnapshots, 0)
	stats.Add(numUserSnapshotsFailed, 0)
	stats.Add(numWALSnapshots, 0)
	stats.Add(numWALSnapshotsFailed, 0)
	stats.Add(numSnapshotsFull, 0)
	stats.Add(numSnapshotsIncremental, 0)
	stats.Add(numFullCheckpointFailed, 0)
	stats.Add(numWALCheckpointTruncateFailed, 0)
	stats.Add(numAutoVacuums, 0)
	stats.Add(numAutoVacuumsFailed, 0)
	stats.Add(autoVacuumDuration, 0)
	stats.Add(numBoots, 0)
	stats.Add(numBackups, 0)
	stats.Add(numLoads, 0)
	stats.Add(numRestores, 0)
	stats.Add(numRestoresFailed, 0)
	stats.Add(numRecoveries, 0)
	stats.Add(numProviderChecks, 0)
	stats.Add(numProviderProvides, 0)
	stats.Add(numProviderProvidesFail, 0)
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
	stats.Add(snapshotCreateChkTruncateDuration, 0)
	stats.Add(snapshotCreateWALCompactDuration, 0)
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
	snapshotDir   string
	peersPath     string
	peersInfoPath string

	restorePath   string
	restoreDoneCh chan struct{}

	raft    *raft.Raft // The consensus mechanism.
	ly      Layer
	raftTn  *NodeTransport
	raftID  string           // Node ID.
	dbConf  *DBConfig        // SQLite database config.
	dbPath  string           // Path to underlying SQLite file.
	walPath string           // Path to WAL file.
	dbDir   string           // Path to directory containing SQLite file.
	db      *sql.SwappableDB // The underlying SQLite store.

	dechunkManager *chunking.DechunkerManager
	cmdProc        *CommandProcessor

	// Channels that must be closed for the Store to be considered ready.
	readyChans             []<-chan struct{}
	numClosedReadyChannels int
	readyChansMu           sync.Mutex

	// Channels for WAL-size triggered snapshotting
	snapshotWClose chan struct{}
	snapshotWDone  chan struct{}

	// Snapshotting synchronization
	queryTxMu   sync.RWMutex
	snapshotCAS *CheckAndSet

	// Latest log entry index actually reflected by the FSM. Due to Raft code
	// this value is not updated after a Snapshot-restore.
	fsmIdx        *atomic.Uint64
	fsmUpdateTime *AtomicTime // This is node-local time.

	// appendedAtTimeis the Leader's clock time when that Leader appended the log entry.
	// The Leader that actually appended the log entry is not necessarily the current Leader.
	appendedAtTime *AtomicTime

	// Latest log entry index which actually changed the database.
	dbAppliedIdx         *atomic.Uint64
	appliedIdxUpdateDone chan struct{}

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

	logger         *log.Logger
	logIncremental bool

	notifyMu        sync.Mutex
	BootstrapExpect int
	bootstrapped    bool
	notifyingNodes  map[string]*Server

	ShutdownOnRemove         bool
	SnapshotThreshold        uint64
	SnapshotThresholdWALSize uint64
	SnapshotInterval         time.Duration
	LeaderLeaseTimeout       time.Duration
	HeartbeatTimeout         time.Duration
	ElectionTimeout          time.Duration
	ApplyTimeout             time.Duration
	RaftLogLevel             string
	NoFreeListSync           bool
	AutoVacInterval          time.Duration

	// Node-reaping configuration
	ReapTimeout         time.Duration
	ReapReadOnlyTimeout time.Duration

	numTrailingLogs uint64

	// For whitebox testing
	numAutoVacuums  int
	numIgnoredJoins int
	numNoops        *atomic.Uint64
	numSnapshotsMu  sync.Mutex
	numSnapshots    int
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
func New(ly Layer, c *Config) *Store {
	logger := c.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[store] ", log.LstdFlags)
	}

	dbPath := filepath.Join(c.Dir, sqliteFile)
	if c.DBConf.OnDiskPath != "" {
		dbPath = c.DBConf.OnDiskPath
	}

	return &Store{
		ly:              ly,
		raftDir:         c.Dir,
		snapshotDir:     filepath.Join(c.Dir, snapshotsDirName),
		peersPath:       filepath.Join(c.Dir, peersPath),
		peersInfoPath:   filepath.Join(c.Dir, peersInfoPath),
		restoreDoneCh:   make(chan struct{}),
		raftID:          c.ID,
		dbConf:          c.DBConf,
		dbPath:          dbPath,
		walPath:         sql.WALPath(dbPath),
		dbDir:           filepath.Dir(dbPath),
		leaderObservers: make([]chan<- struct{}, 0),
		reqMarshaller:   command.NewRequestMarshaler(),
		logger:          logger,
		notifyingNodes:  make(map[string]*Server),
		ApplyTimeout:    applyTimeout,
		snapshotCAS:     NewCheckAndSet(),
		fsmIdx:          &atomic.Uint64{},
		fsmUpdateTime:   NewAtomicTime(),
		appendedAtTime:  NewAtomicTime(),
		dbAppliedIdx:    &atomic.Uint64{},
		numNoops:        &atomic.Uint64{},
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

	s.RegisterReadyChannel(s.restoreDoneCh)
	s.restorePath = path
	return nil
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
	s.logger.Printf("opening store with node ID %s, listening on %s", s.raftID, s.ly.Addr().String())

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
	s.cmdProc = NewCommandProcessor(s.logger, s.dechunkManager)

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
	nt := raft.NewNetworkTransport(NewTransport(s.ly), connectionPoolCount, connectionTimeout, nil)
	s.raftTn = NewNodeTransport(nt)

	// Don't allow control over trailing logs directly, just implement a policy.
	s.numTrailingLogs = uint64(float64(s.SnapshotThreshold) * trailingScale)

	config := s.raftConfig()
	config.LocalID = raft.ServerID(s.raftID)

	// Upgrade any pre-existing snapshots.
	oldSnapshotDir := filepath.Join(s.raftDir, "snapshots")
	if err := snapshot.Upgrade(oldSnapshotDir, s.snapshotDir, s.logger); err != nil {
		return fmt.Errorf("failed to upgrade snapshots: %s", err)
	}

	// Create store for the Snapshots.
	snapshotStore, err := snapshot.NewStore(filepath.Join(s.snapshotDir))
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

	s.db, err = createOnDisk(s.dbPath, s.dbConf.FKConstraints, true)
	if err != nil {
		return fmt.Errorf("failed to create on-disk database: %s", err)
	}

	// Clean up any files from aborted operations. This tries to catch the case where scratch files
	// were created in the Raft directory, not cleaned up, and then the node was restarted with an
	// explicit SQLite path set.
	for _, pattern := range []string{
		restoreScratchPattern,
		bootScatchPattern,
		backupScatchPattern,
		vacuumScatchPattern} {
		for _, dir := range []string{s.raftDir, s.dbDir} {
			files, err := filepath.Glob(filepath.Join(dir, pattern))
			if err != nil {
				return fmt.Errorf("failed to locate temporary files for pattern %s: %s", pattern, err.Error())
			}
			for _, f := range files {
				if err := os.Remove(f); err != nil {
					return fmt.Errorf("failed to remove temporary file %s: %s", f, err.Error())
				}
			}
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

	// WAL-size triggered snapshotting.
	s.snapshotWClose, s.snapshotWDone = s.runWALSnapshotting()

	// Periodically update the applied index for faster startup.
	s.appliedIdxUpdateDone = s.updateAppliedIndex()

	if err := s.initVacuumTime(); err != nil {
		return fmt.Errorf("failed to initialize auto-vacuum times: %s", err.Error())
	}
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

// Committed blocks until the local commit index is greater than or
// equal to the Leader index, as checked when the function is called.
// It returns the committed index. If the Leader index is 0, then the
// system waits until the commit index is at least 1.
func (s *Store) Committed(timeout time.Duration) (uint64, error) {
	lci, err := s.LeaderCommitIndex()
	if err != nil {
		return lci, err
	}
	if lci == 0 {
		lci = 1
	}
	return lci, s.WaitForCommitIndex(lci, timeout)
}

// Close closes the store. If wait is true, waits for a graceful shutdown.
func (s *Store) Close(wait bool) (retErr error) {
	defer func() {
		if retErr == nil {
			s.logger.Printf("store closed with node ID %s, listening on %s", s.raftID, s.ly.Addr().String())
			s.open = false
		}
	}()
	if !s.open {
		// Protect against closing already-closed resource, such as channels.
		return nil
	}

	s.dechunkManager.Close()

	close(s.appliedIdxUpdateDone)
	close(s.observerClose)
	<-s.observerDone

	close(s.snapshotWClose)
	<-s.snapshotWDone

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

// WaitForApplied waits for all Raft log entries to be applied to the
// underlying database.
func (s *Store) WaitForAllApplied(timeout time.Duration) error {
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

// WaitForCommitIndex blocks until the local Raft commit index is equal to
// or greater the given index, or the timeout expires.
func (s *Store) WaitForCommitIndex(idx uint64, timeout time.Duration) error {
	tck := time.NewTicker(commitEquivalenceDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if s.raft.CommitIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// DBAppliedIndex returns the index of the last Raft log that changed the
// underlying database. If the index is unknown then 0 is returned.
func (s *Store) DBAppliedIndex() uint64 {
	return s.dbAppliedIdx.Load()
}

// IsLeader is used to determine if the current node is cluster leader
func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// HasLeader returns true if the cluster has a leader, false otherwise.
func (s *Store) HasLeader() bool {
	return s.raft.Leader() != ""
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

// LastVacuumTime returns the time of the last automatic VACUUM.
func (s *Store) LastVacuumTime() (time.Time, error) {
	return s.getKeyTime(lastVacuumTimeKey)
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

// CommitIndex returns the Raft commit index.
func (s *Store) CommitIndex() (uint64, error) {
	return s.raft.CommitIndex(), nil
}

// LeaderCommitIndex returns the Raft leader commit index, as indicated
// by the latest AppendEntries RPC. If this node is the Leader then the
// commit index is returned directly from the Raft object.
func (s *Store) LeaderCommitIndex() (uint64, error) {
	if s.raft.State() == raft.Leader {
		return s.raft.CommitIndex(), nil
	}
	return s.raftTn.LeaderCommitIndex(), nil
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

// WaitForFSMIndex blocks until a given log index has been applied to our
// state machine or the timeout expires.
func (s *Store) WaitForFSMIndex(idx uint64, timeout time.Duration) (uint64, error) {
	tck := time.NewTicker(appliedWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for {
		select {
		case <-tck.C:
			if fsmIdx := s.fsmIdx.Load(); fsmIdx >= idx {
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
	raftStats["transport"] = s.raftTn.Stats()

	dirSz, err := dirSize(s.raftDir)
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
		"fsm_index":          s.fsmIdx.Load(),
		"fsm_update_time":    s.fsmUpdateTime.Load(),
		"db_applied_index":   s.dbAppliedIdx.Load(),
		"last_applied_index": lAppliedIdx,
		"addr":               s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    leaderAddr,
		},
		"leader_appended_at_time": s.appendedAtTime.Load(),
		"ready":                   s.Ready(),
		"observer": map[string]uint64{
			"observed": s.observer.GetNumObserved(),
			"dropped":  s.observer.GetNumDropped(),
		},
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
		"dir_size_friendly":      friendlyBytes(uint64(dirSz)),
		"sqlite3":                dbStatus,
		"db_conf":                s.dbConf,
	}

	if s.AutoVacInterval > 0 {
		bt, err := s.getKeyTime(baseVacuumTimeKey)
		if err != nil {
			return nil, err
		}

		avm := map[string]interface{}{}
		if lvt, err := s.LastVacuumTime(); err == nil {
			avm["last_vacuum"] = lvt
			bt = lvt
		}
		avm["next_vacuum_after"] = bt.Add(s.AutoVacInterval)
		status["auto_vacuum"] = avm
	}

	// Snapshot stats may be in flux if a snapshot is in progress. Only
	// report them if they are available.
	snapsStats, err := s.snapshotStore.Stats()
	if err == nil {
		status["snapshot_store"] = snapsStats
	}
	return status, nil
}

// Execute executes queries that return no rows, but do modify the database.
func (s *Store) Execute(ex *proto.ExecuteRequest) ([]*proto.ExecuteResult, error) {
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

func (s *Store) execute(ex *proto.ExecuteRequest) ([]*proto.ExecuteResult, error) {
	b, compressed, err := s.tryCompress(ex)
	if err != nil {
		return nil, err
	}

	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_EXECUTE,
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
	r := af.Response().(*fsmExecuteResponse)
	return r.results, r.error
}

// Query executes queries that return rows, and do not modify the database.
func (s *Store) Query(qr *proto.QueryRequest) ([]*proto.QueryRows, error) {
	if !s.open {
		return nil, ErrNotOpen
	}

	if qr.Level == proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG {
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
		c := &proto.Command{
			Type:       proto.Command_COMMAND_TYPE_QUERY,
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
		r := af.Response().(*fsmQueryResponse)
		return r.rows, r.error
	}

	if qr.Level == proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	if qr.Level == proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE && s.isStaleRead(qr.Freshness, qr.FreshnessStrict) {
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
func (s *Store) Request(eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, error) {
	if !s.open {
		return nil, ErrNotOpen
	}
	nRW, _ := s.RORWCount(eqr)
	isLeader := s.raft.State() == raft.Leader

	if nRW == 0 && eqr.Level != proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG {
		// It's a little faster just to do a Query of the DB if we know there is no need
		// for consensus.
		if eqr.Request.Transaction {
			// Transaction requested during query, but not going through consensus. This means
			// we need to block any database serialization during the query.
			s.queryTxMu.RLock()
			defer s.queryTxMu.RUnlock()
		}
		convertFn := func(qr []*proto.QueryRows) []*proto.ExecuteQueryResponse {
			resp := make([]*proto.ExecuteQueryResponse, len(qr))
			for i := range qr {
				resp[i] = &proto.ExecuteQueryResponse{
					Result: &proto.ExecuteQueryResponse_Q{Q: qr[i]},
				}
			}
			return resp
		}
		if eqr.Level == proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE && s.isStaleRead(eqr.Freshness, eqr.FreshnessStrict) {
			return nil, ErrStaleRead
		} else if eqr.Level == proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK {
			if !isLeader {
				return nil, ErrNotLeader
			}
		}
		qr, err := s.db.Query(eqr.Request, eqr.Timings)
		return convertFn(qr), err
	}

	// At least one write in the request, or STRONG consistency requested, so
	// we need to go through consensus. Check that we can do that.
	if !isLeader {
		return nil, ErrNotLeader
	}
	if !s.Ready() {
		return nil, ErrNotReady
	}

	// Send the request through consensus.
	b, compressed, err := s.tryCompress(eqr)
	if err != nil {
		return nil, err
	}
	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_EXECUTE_QUERY,
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
	r := af.Response().(*fsmExecuteQueryResponse)
	return r.results, r.error
}

// Backup writes a consistent snapshot of the underlying database to dst. This
// can be called while writes are being made to the system. The backup may fail
// if the system is actively snapshotting. The client can just retry in this case.
//
// If vacuum is not true the copy is written directly to dst, optionally in compressed
// form, without any intermediate temporary files.
//
// If vacuum is true, then a VACUUM is performed on the database before the backup
// is made. If compression false, and dst is an os.File, then the vacuumed copy
// will be written directly to that file. Otherwise a temporary file will be created,
// and that temporary file copied to dst.
func (s *Store) Backup(br *proto.BackupRequest, dst io.Writer) (retErr error) {
	if !s.open {
		return ErrNotOpen
	}

	if br.Vacuum && br.Format != proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
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

	if br.Format == proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY {
		var srcFD *os.File
		var err error
		if br.Vacuum {
			if !br.Compress {
				if f, ok := dst.(*os.File); ok {
					// Fast path, just vacuum directly to the destination.
					return s.db.Backup(f.Name(), br.Vacuum)
				}
			}

			srcFD, err = os.CreateTemp(s.dbDir, backupScatchPattern)
			if err != nil {
				return err
			}
			defer os.Remove(srcFD.Name())
			defer srcFD.Close()
			if err := s.db.Backup(srcFD.Name(), br.Vacuum); err != nil {
				return err
			}
		} else {
			// Snapshot to ensure the main SQLite file has all the latest data.
			if err := s.Snapshot(0); err != nil {
				if err != raft.ErrNothingNewToSnapshot &&
					!strings.Contains(err.Error(), "wait until the configuration entry at") {
					return fmt.Errorf("pre-backup snapshot failed: %s", err.Error())
				}
			}
			// Pause any snapshotting and which will allow us to read the SQLite
			// file without it changing underneath us. Any new writes will be
			// sent to the WAL.
			if err := s.snapshotCAS.Begin(); err != nil {
				return err
			}
			defer s.snapshotCAS.End()

			// Now we can copy the SQLite file directly.
			srcFD, err = os.Open(s.dbPath)
			if err != nil {
				return fmt.Errorf("failed to open database file: %s", err.Error())
			}
			defer srcFD.Close()
		}

		if br.Compress {
			var dstGz *gzip.Writer
			dstGz, err = gzip.NewWriterLevel(dst, gzip.BestSpeed)
			if err != nil {
				return err
			}
			defer dstGz.Close()
			_, err = io.Copy(dstGz, srcFD)
		} else {
			_, err = io.Copy(dst, srcFD)
		}
		return err
	} else if br.Format == proto.BackupRequest_BACKUP_REQUEST_FORMAT_SQL {
		return s.db.Dump(dst)
	}
	return ErrInvalidBackupFormat
}

// Loads an entire SQLite file into the database, sending the request
// through the Raft log.
func (s *Store) Load(lr *proto.LoadRequest) error {
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
func (s *Store) load(lr *proto.LoadRequest) error {
	startT := time.Now()

	b, err := command.MarshalLoadRequest(lr)
	if err != nil {
		s.logger.Printf("load failed during load-request marshalling %s", err.Error())
		return err
	}

	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_LOAD,
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

	// Write the data to a temporary file.
	f, err := os.CreateTemp(s.dbDir, bootScatchPattern)
	if err != nil {
		return 0, err
	}
	defer os.Remove(f.Name())
	defer f.Close()

	cw := progress.NewCountingWriter(f)
	cm := progress.StartCountingMonitor(func(n int64) {
		s.logger.Printf("boot process installed %d bytes", n)
	}, cw)
	n, err := func() (int64, error) {
		defer cm.StopAndWait()
		defer f.Close()
		return io.Copy(cw, r)
	}()
	if err != nil {
		return n, err
	}

	// Confirm the data is a valid SQLite database.
	if !sql.IsValidSQLiteFile(f.Name()) {
		return n, fmt.Errorf("invalid SQLite data")
	}

	// Raft won't snapshot unless there is at least one unsnappshotted log entry,
	// so prep that now before we do anything destructive.
	if af, err := s.Noop("boot"); err != nil {
		return n, err
	} else if err := af.Error(); err != nil {
		return n, err
	}

	// Swap in new database file.
	if err := s.db.Swap(f.Name(), s.dbConf.FKConstraints, true); err != nil {
		return n, fmt.Errorf("error swapping database file: %v", err)
	}

	// Snapshot, so we load the new database into the Raft system.
	if err := s.snapshotStore.SetFullNeeded(); err != nil {
		return n, err
	}
	if err := s.Snapshot(1); err != nil {
		return n, err
	}
	stats.Add(numBoots, 1)
	return n, nil
}

// Vacuum performs a VACUUM operation on the underlying database. It does
// this by performing a VACUUM INTO a temporary file, and then swapping
// the temporary file with the existing database file. The database is then
// re-opened.
func (s *Store) Vacuum() error {
	fd, err := os.CreateTemp(s.dbDir, vacuumScatchPattern)
	if err != nil {
		return err
	}
	if err := fd.Close(); err != nil {
		return err
	}
	defer os.Remove(fd.Name())
	if err := s.db.VacuumInto(fd.Name()); err != nil {
		return err
	}

	// Verify that the VACUUMed database is valid.
	if !sql.IsValidSQLiteFile(fd.Name()) {
		return fmt.Errorf("invalid SQLite file post VACUUM")
	}

	// Swap in new database file.
	if err := s.db.Swap(fd.Name(), s.dbConf.FKConstraints, true); err != nil {
		return fmt.Errorf("error swapping database file: %v", err)
	}

	if err := s.snapshotStore.SetFullNeeded(); err != nil {
		return err
	}
	return nil
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

// Notify notifies this Store that a node is ready for bootstrapping at the
// given address. Once the number of known nodes reaches the expected level
// bootstrapping will be attempted using this Store. "Expected level" includes
// this node, so this node must self-notify to ensure the cluster bootstraps
// with the *advertised Raft address* which the Store doesn't know about.
//
// Notifying is idempotent. A node may repeatedly notify the Store without issue.
func (s *Store) Notify(nr *proto.NotifyRequest) error {
	if !s.open {
		return ErrNotOpen
	}

	s.notifyMu.Lock()
	defer s.notifyMu.Unlock()

	if s.BootstrapExpect == 0 || s.bootstrapped || s.HasLeader() {
		// There is no reason this node will bootstrap.
		//
		// - Read-only nodes require that BootstrapExpect is set to 0, so this
		// block ensures that notifying a read-only node will not cause a bootstrap.
		// - If the node is already bootstrapped, then there is nothing to do.
		// - If the node already has a leader, then no bootstrapping is required.
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
func (s *Store) Join(jr *proto.JoinRequest) error {
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
func (s *Store) Remove(rn *proto.RemoveNodeRequest) error {
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
func (s *Store) Noop(id string) (raft.ApplyFuture, error) {
	n := &proto.Noop{
		Id: id,
	}
	b, err := command.MarshalNoop(n)
	if err != nil {
		return nil, err
	}

	c := &proto.Command{
		Type:       proto.Command_COMMAND_TYPE_NOOP,
		SubCommand: b,
	}
	bc, err := command.Marshal(c)
	if err != nil {
		return nil, err
	}

	return s.raft.Apply(bc, s.ApplyTimeout), nil
}

// RORWCount returns the number of read-only and read-write statements in the
// given ExecuteQueryRequest.
func (s *Store) RORWCount(eqr *proto.ExecuteQueryRequest) (nRW, nRO int) {
	for _, stmt := range eqr.Request.Statements {
		sql := stmt.Sql
		if sql == "" {
			continue
		}
		ro, err := s.db.StmtReadOnly(sql)
		if err == nil && ro {
			nRO++
		} else {
			nRW++
		}
	}
	return
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

// initVacuumTime initializes the last vacuum times in the Config store.
// If auto-vacuum is disabled, then all auto-vacuum related state is removed.
// If enabled, but no last vacuum time is set, then the auto-bac baseline
// time i.e. now is set. If a last vacuum time is set, then it is left as is.
func (s *Store) initVacuumTime() error {
	if s.AutoVacInterval == 0 {
		if err := s.clearKeyTime(baseVacuumTimeKey); err != nil {
			return fmt.Errorf("failed to clear base vacuum time: %s", err)
		}
		if err := s.clearKeyTime(lastVacuumTimeKey); err != nil {
			return fmt.Errorf("failed to clear last vacuum time: %s", err)
		}
		return nil
	}
	if _, err := s.LastVacuumTime(); err != nil {
		return s.setKeyTime(baseVacuumTimeKey, time.Now())
	}
	return nil
}

func (s *Store) setKeyTime(key string, t time.Time) error {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	if err := binary.Write(buf, binary.LittleEndian, t.UnixNano()); err != nil {
		return err
	}
	return s.boltStore.Set([]byte(key), buf.Bytes())
}

func (s *Store) getKeyTime(key string) (time.Time, error) {
	kt, err := s.boltStore.Get([]byte(key))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %s", key, err)
	} else if kt == nil {
		return time.Time{}, fmt.Errorf("key %s is nil", key)
	}
	n := int64(binary.LittleEndian.Uint64(kt))
	return time.Unix(0, n), nil
}

func (s *Store) clearKeyTime(key string) error {
	return s.boltStore.Set([]byte(key), nil)
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
	opts := hclog.DefaultOptions
	opts.Name = ""
	opts.Level = hclog.LevelFromString(s.RaftLogLevel)
	s.logIncremental = opts.Level < hclog.Warn
	config.Logger = hclog.FromStandardLogger(log.New(os.Stderr, "[raft] ", log.LstdFlags), opts)
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

func (s *Store) isStaleRead(freshness int64, strict bool) bool {
	if s.raft.State() == raft.Leader {
		return false
	}
	return IsStaleRead(
		s.raft.LastContact(),
		s.fsmUpdateTime.Load(),
		s.appendedAtTime.Load(),
		s.fsmIdx.Load(),
		s.raftTn.CommandCommitIndex(),
		freshness,
		strict)
}

type fsmExecuteResponse struct {
	results []*proto.ExecuteResult
	error   error
}

type fsmQueryResponse struct {
	rows  []*proto.QueryRows
	error error
}

type fsmExecuteQueryResponse struct {
	results []*proto.ExecuteQueryResponse
	error   error
}

type fsmGenericResponse struct {
	error error
}

// fsmApply applies a Raft log entry to the database.
func (s *Store) fsmApply(l *raft.Log) (e interface{}) {
	defer func() {
		s.fsmIdx.Store(l.Index)
		s.fsmUpdateTime.Store(time.Now())
		s.appendedAtTime.Store(l.AppendedAt)
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

	cmd, mutated, r := s.cmdProc.Process(l.Data, s.db)
	if mutated {
		s.dbAppliedIdx.Store(l.Index)
	}
	if cmd.Type == proto.Command_COMMAND_TYPE_NOOP {
		s.numNoops.Add(1)
	} else if cmd.Type == proto.Command_COMMAND_TYPE_LOAD {
		// Swapping in a new database invalidates any existing snapshot.
		err := s.snapshotStore.SetFullNeeded()
		if err != nil {
			return &fsmGenericResponse{
				error: fmt.Errorf("failed to set full snapshot needed: %s", err.Error()),
			}
		}
	}
	return r
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
func (s *Store) fsmSnapshot() (fSnap raft.FSMSnapshot, retErr error) {
	s.queryTxMu.Lock()
	defer s.queryTxMu.Unlock()

	if err := s.snapshotCAS.Begin(); err != nil {
		return nil, err
	}
	defer s.snapshotCAS.End()

	startT := time.Now()
	defer func() {
		if retErr != nil {
			stats.Add(numSnapshotsFailed, 1)
		}
	}()

	// Automatic VACUUM needed? This is deliberately done in the context of a Snapshot
	// as it guarantees that the database is not being written to.
	if avn, err := s.autoVacNeeded(time.Now()); err != nil {
		return nil, err
	} else if avn {
		vacStart := time.Now()
		if err := s.Vacuum(); err != nil {
			stats.Add(numAutoVacuumsFailed, 1)
			return nil, err
		}
		s.logger.Printf("database vacuumed in %s", time.Since(vacStart))
		stats.Get(autoVacuumDuration).(*expvar.Int).Set(time.Since(vacStart).Milliseconds())
		stats.Add(numAutoVacuums, 1)
		s.numAutoVacuums++
		if err := s.setKeyTime(lastVacuumTimeKey, time.Now()); err != nil {
			return nil, err
		}
	}

	fullNeeded, err := s.snapshotStore.FullNeeded()
	if err != nil {
		return nil, err
	}
	fPLog := fullPretty(fullNeeded)
	defer func() {
		s.numSnapshotsMu.Lock()
		defer s.numSnapshotsMu.Unlock()
		s.numSnapshots++
	}()

	var fsmSnapshot raft.FSMSnapshot
	if fullNeeded {
		chkStartTime := time.Now()
		if err := s.db.Checkpoint(sql.CheckpointTruncate); err != nil {
			stats.Add(numFullCheckpointFailed, 1)
			return nil, err
		}
		stats.Get(snapshotCreateChkTruncateDuration).(*expvar.Int).Set(time.Since(chkStartTime).Milliseconds())
		dbFD, err := os.Open(s.db.Path())
		if err != nil {
			return nil, err
		}
		fsmSnapshot = snapshot.NewSnapshot(dbFD)
		stats.Add(numSnapshotsFull, 1)
	} else {
		compactedBuf := bytes.NewBuffer(nil)
		var err error
		if pathExistsWithData(s.walPath) {
			compactStartTime := time.Now()
			// Read a compacted version of the WAL into memory, and write it
			// to the Snapshot store.
			walFD, err := os.Open(s.walPath)
			if err != nil {
				return nil, err
			}
			defer walFD.Close()
			scanner, err := wal.NewFastCompactingScanner(walFD)
			if err != nil {
				return nil, err
			}
			compactedBytes, err := scanner.Bytes()
			if err != nil {
				return nil, err
			}
			stats.Get(snapshotCreateWALCompactDuration).(*expvar.Int).Set(time.Since(compactStartTime).Milliseconds())
			compactedBuf = bytes.NewBuffer(compactedBytes)

			// Now that we're written a (compacted) copy of the WAL to the Snapshot,
			// we can truncate the WAL. We use truncate mode so that the next WAL
			// contains just changes since the this snapshot.
			walSz, err := fileSize(s.walPath)
			if err != nil {
				return nil, err
			}
			chkTStartTime := time.Now()
			if err := s.db.Checkpoint(sql.CheckpointTruncate); err != nil {
				stats.Add(numWALCheckpointTruncateFailed, 1)
				return nil, fmt.Errorf("snapshot can't complete due to WAL checkpoint failure (will retry): %s",
					err.Error())
			}
			stats.Get(snapshotCreateChkTruncateDuration).(*expvar.Int).Set(time.Since(chkTStartTime).Milliseconds())
			stats.Get(snapshotWALSize).(*expvar.Int).Set(int64(compactedBuf.Len()))
			stats.Get(snapshotPrecompactWALSize).(*expvar.Int).Set(walSz)
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
	fs := FSMSnapshot{
		FSMSnapshot: fsmSnapshot,
	}
	if fullNeeded || s.logIncremental {
		s.logger.Printf("%s snapshot created in %s on node ID %s", fPLog, dur, s.raftID)
		fs.logger = s.logger
	}
	return &fs, nil
}

// fsmRestore restores the node to a previous state. The Hashicorp docs state this
// will not be called concurrently with Apply(), so synchronization with Execute()
// is not necessary.
func (s *Store) fsmRestore(rc io.ReadCloser) (retErr error) {
	defer func() {
		if retErr != nil {
			stats.Add(numRestoresFailed, 1)
		}
	}()
	s.logger.Printf("initiating node restore on node ID %s", s.raftID)
	startT := time.Now()

	// Create a scatch file to write the restore data to it.
	tmpFile, err := os.CreateTemp(s.dbDir, restoreScratchPattern)
	if err != nil {
		return fmt.Errorf("error creating temporary file for restore operation: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy it from the reader to the temporary file.
	_, err = io.Copy(tmpFile, rc)
	if err != nil {
		return fmt.Errorf("error copying restore data: %v", err)
	}
	if tmpFile.Close(); err != nil {
		return fmt.Errorf("error creating temporary file for restore operation: %v", err)
	}

	if err := s.db.Swap(tmpFile.Name(), s.dbConf.FKConstraints, true); err != nil {
		return fmt.Errorf("error swapping database file: %v", err)
	}
	s.logger.Printf("successfully opened database at %s due to restore", s.db.Path())

	// Take conservative approach and assume that everything has changed, so update
	// the indexes. It is possible that dbAppliedIdx is now ahead of some other nodes'
	// same value, since the last index is not necessarily a database-changing index,
	// but that is OK. Worse that can happen is that anything paying attention to the
	// index might consider the database to be changed when it is not, *logically* speaking.
	li, err := snapshot.LatestIndex(s.snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot index post restore: %s", err)
	}
	if err := s.boltStore.SetAppliedIndex(li); err != nil {
		return fmt.Errorf("failed to set applied index: %s", err)
	}
	s.fsmIdx.Store(li)
	s.dbAppliedIdx.Store(li)

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

// Snapshot performs a snapshot, leaving n trailing logs behind. If n
// is greater than zero, that many logs are left in the log after
// snapshotting. If n is zero, then the number set at Store creation is used.
// Finally, once this function returns, the trailing log configuration value
// is reset to the value set at Store creation.
func (s *Store) Snapshot(n uint64) (retError error) {
	defer func() {
		if retError != nil {
			stats.Add(numUserSnapshotsFailed, 1)
		}
	}()

	if n > 0 {
		cfg := s.raft.ReloadableConfig()
		defer func() {
			cfg.TrailingLogs = s.numTrailingLogs
			if err := s.raft.ReloadConfig(cfg); err != nil {
				s.logger.Printf("failed to reload Raft config: %s", err.Error())
			}
		}()
		cfg.TrailingLogs = n
		if err := s.raft.ReloadConfig(cfg); err != nil {
			return fmt.Errorf("failed to reload Raft config: %s", err.Error())
		}
	}
	if err := s.raft.Snapshot().Error(); err != nil {
		if strings.Contains(err.Error(), ErrLoadInProgress.Error()) {
			return ErrLoadInProgress
		}
		return err
	}
	stats.Add(numUserSnapshots, 1)
	return nil
}

// runWALSnapshotting runs the periodic check to see if a snapshot should be
// triggered due to WAL size.
func (s *Store) runWALSnapshotting() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})
	ticker := time.NewTicker(time.Hour) // Just need an initialized ticker to start with.
	ticker.Stop()
	if s.SnapshotInterval > 0 && s.SnapshotThresholdWALSize > 0 {
		ticker.Reset(random.Jitter(s.SnapshotInterval))
	}

	go func() {
		defer close(doneCh)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sz, err := fileSizeExists(s.walPath)
				if err != nil {
					s.logger.Printf("failed to check WAL size: %s", err.Error())
					continue
				}
				if uint64(sz) >= s.SnapshotThresholdWALSize {
					if err := s.Snapshot(0); err != nil {
						stats.Add(numWALSnapshotsFailed, 1)
						s.logger.Printf("failed to snapshot due to WAL threshold: %s", err.Error())
					}
					stats.Add(numWALSnapshots, 1)
				}
			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
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
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	lr := &proto.LoadRequest{
		Data: b,
	}
	return s.load(lr)
}

// logSize returns the size of the Raft log on disk.
func (s *Store) logSize() (int64, error) {
	fi, err := os.Stat(filepath.Join(s.raftDir, raftDBPath))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
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

// autoVacNeeded returns true if an automatic VACUUM is needed.
func (s *Store) autoVacNeeded(t time.Time) (bool, error) {
	if s.AutoVacInterval == 0 {
		return false, nil
	}
	vt, err := s.LastVacuumTime()
	if err == nil {
		return t.Sub(vt) > s.AutoVacInterval, nil
	}
	// OK, check if we have a base time from which we can start.
	bt, err := s.getKeyTime(baseVacuumTimeKey)
	if err != nil {
		return false, err
	}
	return t.Sub(bt) > s.AutoVacInterval, nil
}

// createOnDisk opens an on-disk database file at the configured path. Any
// preexisting file will be removed before the database is opened.
func createOnDisk(path string, fkConstraints, wal bool) (*sql.SwappableDB, error) {
	if err := sql.RemoveFiles(path); err != nil {
		return nil, err
	}
	return sql.OpenSwappable(path, fkConstraints, wal)
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

// fileSizeExists returns the size of the given file, or 0 if the file does not
// exist. Any other error is returned.
func fileSizeExists(path string) (int64, error) {
	if !pathExists(path) {
		return 0, nil
	}
	return fileSize(path)
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

func friendlyBytes(n uint64) string {
	return humanize.Bytes(n)
}
