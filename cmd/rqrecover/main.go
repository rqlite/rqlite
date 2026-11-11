// Command rqrecover inspects the data directory of a stopped rqlite node, and
// reports what an operator needs to know when recovering a cluster that has
// lost quorum. It can also generate the peers file which will bring the node
// back online as a single-node cluster.
//
// rqrecover never modifies the data directory.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/rqlite/raft-boltdb/v2"
	"github.com/rqlite/rqlite/v10/snapshot"
	"go.etcd.io/bbolt"
)

// Names of the files and directories within an rqlite data directory.
const (
	raftDBName       = "raft.db"
	snapshotsDirName = "wsnapshots"
	sqliteName       = "db.sqlite"
	walName          = "db.sqlite-wal"
	peersPath        = "raft/peers.json"

	openTimeout = 2 * time.Second
)

// snapshotInfo describes a single snapshot in the snapshot store.
type snapshotInfo struct {
	ID    string
	Full  bool
	Index uint64
	Term  uint64
}

// nodeInfo is what can be learned about a node from its data directory.
type nodeInfo struct {
	// Servers is the most recent cluster membership the node knows of. A data
	// directory does not record which member the node itself is, so when
	// there is more than one server the operator must say.
	Servers []raft.Server

	FirstIndex      uint64
	LastIndex       uint64
	PendingCommands uint64

	Snapshots []snapshotInfo

	SQLiteSize int64
	WALSize    int64
}

// peer is an entry in the peers file read by rqlite at startup.
type peer struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	NonVoter bool   `json:"non_voter"`
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <data directory>\n\n", filepath.Base(os.Args[0]))
		fmt.Fprintln(os.Stderr, "Reports the state of the stopped rqlite node which uses the given data")
		fmt.Fprintln(os.Stderr, "directory, and optionally generates the peers file needed to recover it.")
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(flag.Arg(0), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "rqrecover: %s\n", err)
		os.Exit(1)
	}
}

func run(dir string, in io.Reader, out io.Writer) error {
	info, err := inspect(dir)
	if err != nil {
		return err
	}
	stdin := bufio.NewReader(in)

	self, err := chooseSelf(info.Servers, stdin, out)
	if err != nil {
		return err
	}

	var fulls int
	for _, s := range info.Snapshots {
		if s.Full {
			fulls++
		}
	}
	fmt.Fprintf(out, "Node ID:           %s\n", self.ID)
	fmt.Fprintf(out, "Raft address:      %s\n", self.Address)
	fmt.Fprintf(out, "Pending commands:  %d (Raft log holds indexes %d to %d)\n",
		info.PendingCommands, info.FirstIndex, info.LastIndex)
	fmt.Fprintf(out, "Snapshots:         %d (%d full, %d incremental)\n",
		len(info.Snapshots), fulls, len(info.Snapshots)-fulls)
	for _, s := range info.Snapshots {
		typ := "incremental"
		if s.Full {
			typ = "full"
		}
		fmt.Fprintf(out, "                   %s  %-11s  index %d, term %d\n", s.ID, typ, s.Index, s.Term)
	}
	fmt.Fprintf(out, "SQLite database:   %s (WAL %s)\n", size(info.SQLiteSize), size(info.WALSize))

	fmt.Fprintf(out, "\nGenerate the peers file which brings this node back online by itself? [y/N] ")
	answer, err := stdin.ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
	default:
		return nil
	}

	// A node recovered by itself must be a voter, whatever it was before.
	b, err := json.MarshalIndent([]peer{{ID: string(self.ID), Address: string(self.Address)}}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "\n%s\n\n", b)
	fmt.Fprintf(out, "Save this as %s, then start the node.\n", filepath.Join(dir, peersPath))
	return nil
}

// chooseSelf returns the server which is the node being inspected, asking the
// operator when the membership alone cannot answer that.
func chooseSelf(servers []raft.Server, in *bufio.Reader, out io.Writer) (raft.Server, error) {
	if len(servers) == 1 {
		return servers[0], nil
	}
	fmt.Fprintln(out, "This node was last part of a cluster with the following members:")
	fmt.Fprintln(out)
	for i, s := range servers {
		fmt.Fprintf(out, "  %d) ID %s, Raft address %s, %s\n", i+1, s.ID, s.Address, strings.ToLower(s.Suffrage.String()))
	}
	fmt.Fprintf(out, "\nThe data directory does not record which one it is. Which is this node? [1-%d] ", len(servers))
	answer, err := in.ReadString('\n')
	if err != nil && err != io.EOF {
		return raft.Server{}, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(answer))
	if err != nil || n < 1 || n > len(servers) {
		return raft.Server{}, fmt.Errorf("%q is not a number between 1 and %d", strings.TrimSpace(answer), len(servers))
	}
	fmt.Fprintln(out)
	return servers[n-1], nil
}

// inspect reads the data directory at dir, without modifying it.
func inspect(dir string) (*nodeInfo, error) {
	info := &nodeInfo{}

	// Snapshots, oldest first. A node which has never snapshotted may have no
	// snapshot directory at all.
	var newest *raft.SnapshotMeta
	set, err := (&snapshot.SnapshotCatalog{}).Scan(filepath.Join(dir, snapshotsDirName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("failed to read snapshots: %w", err)
	}
	fulls := make(map[string]bool)
	for _, id := range set.Fulls().IDs() {
		fulls[id] = true
	}
	for _, m := range set.RaftMetas() {
		info.Snapshots = append(info.Snapshots, snapshotInfo{
			ID:    m.ID,
			Full:  fulls[m.ID],
			Index: m.Index,
			Term:  m.Term,
		})
		newest = m
	}

	// The Raft log. Opening it read-only, with a timeout, means a running node
	// is detected instead of interfered with.
	logs, err := raftboltdb.New(raftboltdb.Options{
		Path:        filepath.Join(dir, raftDBName),
		BoltOptions: &bbolt.Options{ReadOnly: true, Timeout: openTimeout},
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%s is not an rqlite data directory: no %s found", dir, raftDBName)
		}
		return nil, fmt.Errorf("failed to open Raft log (is the node still running?): %w", err)
	}
	defer logs.Close()

	if info.FirstIndex, err = logs.FirstIndex(); err != nil {
		return nil, fmt.Errorf("failed to get first index of Raft log: %w", err)
	}
	if info.LastIndex, err = logs.LastIndex(); err != nil {
		return nil, fmt.Errorf("failed to get last index of Raft log: %w", err)
	}

	// Commands are pending if no snapshot contains them, as those are the
	// commands which must be replayed to rebuild the database. The newest
	// membership change in the log, if there is one, supersedes the
	// membership in the newest snapshot.
	var snapshotIndex uint64
	if newest != nil {
		snapshotIndex = newest.Index
		info.Servers = newest.Configuration.Servers
	}
	var haveLogConfig bool
	for i := info.LastIndex; i >= info.FirstIndex && i > 0; i-- {
		var l raft.Log
		if err := logs.GetLog(i, &l); err != nil {
			return nil, fmt.Errorf("failed to read Raft log entry at index %d: %w", i, err)
		}
		switch l.Type {
		case raft.LogCommand:
			if i > snapshotIndex {
				info.PendingCommands++
			}
		case raft.LogConfiguration:
			if !haveLogConfig {
				c, err := decodeConfiguration(l.Data)
				if err != nil {
					return nil, fmt.Errorf("failed to decode cluster membership at index %d: %w", i, err)
				}
				info.Servers = c.Servers
				haveLogConfig = true
			}
		}
	}
	if len(info.Servers) == 0 {
		return nil, fmt.Errorf("no cluster membership found in %s: the node was never part of a cluster", dir)
	}

	if info.SQLiteSize, err = fileSize(filepath.Join(dir, sqliteName)); err != nil {
		return nil, err
	}
	if info.WALSize, err = fileSize(filepath.Join(dir, walName)); err != nil {
		return nil, err
	}
	return info, nil
}

// decodeConfiguration decodes a cluster membership stored in the Raft log.
func decodeConfiguration(b []byte) (c raft.Configuration, err error) {
	// The Raft library panics when it cannot decode a membership.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	return raft.DecodeConfiguration(b), nil
}

// fileSize returns the size of the file at path, or zero if there is no such file.
func fileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return fi.Size(), nil
}

// size formats a number of bytes for an operator.
func size(n int64) string {
	const units = "KMGT"
	if n < 1024 {
		return fmt.Sprintf("%d bytes", n)
	}
	f, u := float64(n)/1024, 0
	for f >= 1024 && u < len(units)-1 {
		f /= 1024
		u++
	}
	return fmt.Sprintf("%d bytes, %.1f %ciB", n, f, units[u])
}
