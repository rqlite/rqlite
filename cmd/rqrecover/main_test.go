package main

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/rqlite/raft-boltdb/v2"
	"github.com/rqlite/rqlite/v10/snapshot"
)

const testDB = "../../snapshot/testdata/db-and-wals/backup.db"

func Test_InspectNotDataDir(t *testing.T) {
	if _, err := inspect(t.TempDir()); err == nil {
		t.Fatalf("expected error inspecting empty directory")
	}
}

// Test_SingleNode tests a node which has never snapshotted, so all commands
// in its log are pending and its membership comes from the log.
func Test_SingleNode(t *testing.T) {
	dir := t.TempDir()
	mustWriteLogs(t, dir,
		configLog(1, raft.Server{ID: "node1", Address: "localhost:4002"}),
		&raft.Log{Index: 2, Term: 1, Type: raft.LogNoop},
		&raft.Log{Index: 3, Term: 1, Type: raft.LogCommand},
		&raft.Log{Index: 4, Term: 1, Type: raft.LogCommand},
	)

	info, err := inspect(dir)
	if err != nil {
		t.Fatalf("failed to inspect: %s", err)
	}
	if info.FirstIndex != 1 || info.LastIndex != 4 {
		t.Fatalf("wrong indexes, got %d to %d", info.FirstIndex, info.LastIndex)
	}
	if info.PendingCommands != 2 {
		t.Fatalf("expected 2 pending commands, got %d", info.PendingCommands)
	}
	if len(info.Snapshots) != 0 {
		t.Fatalf("expected no snapshots, got %d", len(info.Snapshots))
	}

	var out bytes.Buffer
	if err := run(dir, strings.NewReader("1\n"), &out); err != nil {
		t.Fatalf("failed to run: %s", err)
	}
	mustHavePeers(t, out.String(), peer{ID: "node1", Address: "localhost:4002"})

	// Declining must not generate anything.
	out.Reset()
	if err := run(dir, strings.NewReader("\n"), &out); err != nil {
		t.Fatalf("failed to run: %s", err)
	}
	if strings.Contains(out.String(), `"id"`) {
		t.Fatalf("peers generated though declined:\n%s", out.String())
	}
}

// Test_MultiNodeWithSnapshot tests a node from a 3-node cluster, where a
// snapshot covers some of the log, and the membership in the log is newer
// than that in the snapshot.
func Test_MultiNodeWithSnapshot(t *testing.T) {
	dir := t.TempDir()
	n1 := raft.Server{ID: "node1", Address: "host1:4002"}
	n2 := raft.Server{ID: "node2", Address: "host2:4002"}
	n3 := raft.Server{ID: "node3", Address: "host3:4002", Suffrage: raft.Nonvoter}

	mustWriteSnapshot(t, dir, 4, 2, n1, n2)
	mustWriteLogs(t, dir,
		&raft.Log{Index: 3, Term: 2, Type: raft.LogCommand},
		&raft.Log{Index: 4, Term: 2, Type: raft.LogCommand},
		&raft.Log{Index: 5, Term: 2, Type: raft.LogCommand},
		configLog(6, n1, n2, n3),
		&raft.Log{Index: 7, Term: 2, Type: raft.LogCommand},
	)

	info, err := inspect(dir)
	if err != nil {
		t.Fatalf("failed to inspect: %s", err)
	}
	if info.PendingCommands != 2 {
		t.Fatalf("expected 2 pending commands, got %d", info.PendingCommands)
	}
	if len(info.Snapshots) != 1 || !info.Snapshots[0].Full || info.Snapshots[0].Index != 4 {
		t.Fatalf("wrong snapshots: %+v", info.Snapshots)
	}
	if len(info.Servers) != 3 {
		t.Fatalf("expected membership from log with 3 servers, got %+v", info.Servers)
	}
	if info.SQLiteSize != 0 {
		t.Fatalf("expected no SQLite database, got size %d", info.SQLiteSize)
	}

	// The operator says it is the third node, the non-voter, which must
	// become a voter to run by itself.
	var out bytes.Buffer
	if err := run(dir, strings.NewReader("1\n3\n"), &out); err != nil {
		t.Fatalf("failed to run: %s", err)
	}
	mustHavePeers(t, out.String(), peer{ID: "node3", Address: "host3:4002"})

	// Full-cluster recovery includes every member, as it was.
	out.Reset()
	if err := run(dir, strings.NewReader("2\n"), &out); err != nil {
		t.Fatalf("failed to run: %s", err)
	}
	for _, s := range []string{"host1:4002", "host2:4002", "host3:4002"} {
		if !strings.Contains(out.String(), "Raft address "+s) {
			t.Fatalf("member %s not listed in output:\n%s", s, out.String())
		}
	}
	mustHavePeers(t, out.String(),
		peer{ID: "node1", Address: "host1:4002"},
		peer{ID: "node2", Address: "host2:4002"},
		peer{ID: "node3", Address: "host3:4002", NonVoter: true})

	if err := run(dir, strings.NewReader("1\n4\n"), io.Discard); err == nil {
		t.Fatalf("expected error choosing a node which does not exist")
	}
}

func configLog(index uint64, servers ...raft.Server) *raft.Log {
	return &raft.Log{
		Index: index,
		Term:  1,
		Type:  raft.LogConfiguration,
		Data:  raft.EncodeConfiguration(raft.Configuration{Servers: servers}),
	}
}

func mustWriteLogs(t *testing.T, dir string, logs ...*raft.Log) {
	t.Helper()
	bs, err := raftboltdb.NewBoltStore(filepath.Join(dir, raftDBName))
	if err != nil {
		t.Fatalf("failed to create Raft log: %s", err)
	}
	defer bs.Close()
	if err := bs.StoreLogs(logs); err != nil {
		t.Fatalf("failed to store logs: %s", err)
	}
}

func mustWriteSnapshot(t *testing.T, dir string, index, term uint64, servers ...raft.Server) {
	t.Helper()
	store, err := snapshot.NewStore(filepath.Join(dir, snapshotsDirName))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %s", err)
	}
	defer store.Close()
	sink, err := store.Create(1, index, term, raft.Configuration{Servers: servers}, 1, nil)
	if err != nil {
		t.Fatalf("failed to create snapshot sink: %s", err)
	}
	streamer, err := snapshot.NewSnapshotStreamer(testDB)
	if err != nil {
		t.Fatalf("failed to create snapshot streamer: %s", err)
	}
	defer streamer.Close()
	if err := streamer.Open(); err != nil {
		t.Fatalf("failed to open snapshot streamer: %s", err)
	}
	if _, err := io.Copy(sink, streamer); err != nil {
		t.Fatalf("failed to write snapshot: %s", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("failed to close snapshot sink: %s", err)
	}
}

// mustHavePeers checks that the output contains a peers file holding exactly
// the given peers.
func mustHavePeers(t *testing.T, out string, exp ...peer) {
	t.Helper()
	start, end := strings.Index(out, "["+"\n"), strings.LastIndex(out, "\n]")
	if start < 0 || end < 0 {
		t.Fatalf("no peers found in output:\n%s", out)
	}
	var peers []peer
	if err := json.Unmarshal([]byte(out[start:end+2]), &peers); err != nil {
		t.Fatalf("failed to unmarshal peers: %s\n%s", err, out)
	}
	if !slices.Equal(peers, exp) {
		t.Fatalf("wrong peers, exp %+v, got %+v", exp, peers)
	}
}
