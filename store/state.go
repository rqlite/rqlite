package store

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/command/chunking"
	sql "github.com/rqlite/rqlite/v8/db"
	rlog "github.com/rqlite/rqlite/v8/log"
	"github.com/rqlite/rqlite/v8/snapshot"
)

// IsStaleRead returns whether a read is stale.
func IsStaleRead(
	leaderlastContact time.Time,
	lastFSMUpdateTime time.Time,
	lastAppendedAtTime time.Time,
	fsmIndex uint64,
	commitIndex uint64,
	freshness int64,
	strict bool,
) bool {
	if freshness == 0 {
		// Freshness not set, so no read can be stale.
		return false
	}
	if time.Since(leaderlastContact).Nanoseconds() > freshness {
		// The Leader has not been in contact witin the freshness window, so
		// the read is stale.
		return true
	}
	if !strict {
		// Strict mode is not enabled, so no further checks are needed.
		return false
	}
	if lastAppendedAtTime.IsZero() {
		// We've yet to be told about any appended log entries, so we
		// assume we're caught up.
		return false
	}
	if fsmIndex == commitIndex {
		// FSM index is the same as the commit index, so we're caught up.
		return false
	}
	// OK, we're not caught up. So was the log that last updated our local FSM
	// appended by the Leader to its log within the freshness window?
	return lastFSMUpdateTime.Sub(lastAppendedAtTime).Nanoseconds() > freshness
}

// IsNewNode returns whether a node using raftDir would be a brand-new node.
// It also means that the window for this node joining a different cluster has passed.
func IsNewNode(raftDir string) bool {
	// If there is any preexisting Raft state, then this node
	// has already been created.
	return !pathExists(filepath.Join(raftDir, raftDBPath))
}

// HasData returns true if the given dir indicates that at least one FSM entry
// has been committed to the log. This is true if there are any snapshots, or
// if there are any entries in the log of raft.LogCommand type. This function
// will block if the Bolt database is already open.
func HasData(dir string) (bool, error) {
	if !dirExists(dir) {
		return false, nil
	}
	sstr, err := snapshot.NewStore(filepath.Join(dir, snapshotsDirName))
	if err != nil {
		return false, err
	}
	snaps, err := sstr.List()
	if err != nil {
		return false, err
	}
	if len(snaps) > 0 {
		return true, nil
	}
	logs, err := rlog.New(filepath.Join(dir, raftDBPath), false)
	if err != nil {
		return false, err
	}
	defer logs.Close()
	h, err := logs.HasCommand()
	if err != nil {
		return false, err
	}
	return h, nil
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
	db, err := sql.OpenSwappable(tmpDBPath, false, true)
	if err != nil {
		return fmt.Errorf("failed to open temporary database: %s", err)
	}
	defer db.Close()

	// Need a dechunker manager to handle any chunked load requests.
	decMgmr, err := chunking.NewDechunkerManager(dataDir)
	if err != nil {
		return fmt.Errorf("failed to create dechunker manager: %s", err.Error())
	}
	cmdProc := NewCommandProcessor(logger, decMgmr)

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
			cmdProc.Process(entry.Data, db)
		}
		lastIndex = entry.Index
		lastTerm = entry.Term
	}

	// Create a new snapshot, placing the configuration in as if it was
	// committed at index 1.
	if err := db.Checkpoint(sql.CheckpointTruncate); err != nil {
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
	return nil
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
