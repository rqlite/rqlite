package raft

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

// CheckInteg will skip a test if integration testing is not enabled.
func CheckInteg(t *testing.T) {
	if !IsInteg() {
		t.SkipNow()
	}
}

// IsInteg returns a boolean telling you if we're in integ testing mode.
func IsInteg() bool {
	return os.Getenv("INTEG_TESTS") != ""
}

type RaftEnv struct {
	dir      string
	conf     *Config
	fsm      *MockFSM
	store    *InmemStore
	snapshot *FileSnapshotStore
	peers    *JSONPeers
	trans    *NetworkTransport
	raft     *Raft
	logger   *log.Logger
}

// Release shuts down and cleans up any stored data, its not restartable after this
func (r *RaftEnv) Release() {
	r.Shutdown()
	os.RemoveAll(r.dir)
}

// Shutdown shuts down raft & transport, but keeps track of its data, its restartable
// after a Shutdown() by calling Start()
func (r *RaftEnv) Shutdown() {
	r.logger.Printf("[WARN] Shutdown node at %v", r.raft.localAddr)
	f := r.raft.Shutdown()
	if err := f.Error(); err != nil {
		panic(err)
	}
	r.trans.Close()
}

// Restart will start a raft node that was previously Shutdown()
func (r *RaftEnv) Restart(t *testing.T) {
	trans, err := NewTCPTransport(r.raft.localAddr, nil, 2, time.Second, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	r.trans = trans
	r.logger.Printf("[INFO] Starting node at %v", trans.LocalAddr())
	raft, err := NewRaft(r.conf, r.fsm, r.store, r.store, r.snapshot, r.peers, r.trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	r.raft = raft
}

func MakeRaft(t *testing.T, conf *Config) *RaftEnv {
	// Set the config
	if conf == nil {
		conf = inmemConfig(t)
	}

	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	stable := NewInmemStore()

	snap, err := NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	env := &RaftEnv{
		conf:     conf,
		dir:      dir,
		store:    stable,
		snapshot: snap,
		fsm:      &MockFSM{},
	}

	trans, err := NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	env.logger = log.New(os.Stdout, trans.LocalAddr()+" :", log.Lmicroseconds)
	env.trans = trans

	env.peers = NewJSONPeers(dir, trans)

	env.logger.Printf("[INFO] Starting node at %v", trans.LocalAddr())
	conf.Logger = env.logger
	raft, err := NewRaft(conf, env.fsm, stable, stable, snap, env.peers, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	env.raft = raft
	return env
}

func WaitFor(env *RaftEnv, state RaftState) error {
	limit := time.Now().Add(200 * time.Millisecond)
	for env.raft.State() != state {
		if time.Now().Before(limit) {
			time.Sleep(10 * time.Millisecond)
		} else {
			return fmt.Errorf("failed to transition to state %v", state)
		}
	}
	return nil
}

func WaitForAny(state RaftState, envs []*RaftEnv) (*RaftEnv, error) {
	limit := time.Now().Add(200 * time.Millisecond)
CHECK:
	for _, env := range envs {
		if env.raft.State() == state {
			return env, nil
		}
	}
	if time.Now().Before(limit) {
		goto WAIT
	}
	return nil, fmt.Errorf("failed to find node in %v state", state)
WAIT:
	time.Sleep(10 * time.Millisecond)
	goto CHECK
}

func WaitFuture(f Future, t *testing.T) error {
	timer := time.AfterFunc(200*time.Millisecond, func() {
		panic(fmt.Errorf("timeout waiting for future %v", f))
	})
	defer timer.Stop()
	return f.Error()
}

func NoErr(err error, t *testing.T) {
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func CheckConsistent(envs []*RaftEnv, t *testing.T) {
	limit := time.Now().Add(400 * time.Millisecond)
	first := envs[0]
	first.fsm.Lock()
	defer first.fsm.Unlock()
	var err error
CHECK:
	l1 := len(first.fsm.logs)
	for i := 1; i < len(envs); i++ {
		env := envs[i]
		env.fsm.Lock()
		l2 := len(env.fsm.logs)
		if l1 != l2 {
			err = fmt.Errorf("log length mismatch %d %d", l1, l2)
			env.fsm.Unlock()
			goto ERR
		}
		for idx, log := range first.fsm.logs {
			other := env.fsm.logs[idx]
			if bytes.Compare(log, other) != 0 {
				err = fmt.Errorf("log entry %d mismatch between %s/%s : '%s' / '%s'", idx, first.raft.localAddr, env.raft.localAddr, log, other)
				env.fsm.Unlock()
				goto ERR
			}
		}
		env.fsm.Unlock()
	}
	return
ERR:
	if time.Now().After(limit) {
		t.Fatalf("%v", err)
	}
	first.fsm.Unlock()
	time.Sleep(20 * time.Millisecond)
	first.fsm.Lock()
	goto CHECK
}

// return a log entry that's at least sz long that has the prefix 'test i '
func logBytes(i, sz int) []byte {
	var logBuffer bytes.Buffer
	fmt.Fprintf(&logBuffer, "test %d ", i)
	for logBuffer.Len() < sz {
		logBuffer.WriteByte('x')
	}
	return logBuffer.Bytes()

}

// Tests Raft by creating a cluster, growing it to 5 nodes while
// causing various stressful conditions
func TestRaft_Integ(t *testing.T) {
	CheckInteg(t)
	conf := DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.SnapshotThreshold = 100
	conf.TrailingLogs = 10
	conf.EnableSingleNode = true

	// Create a single node
	env1 := MakeRaft(t, conf)
	NoErr(WaitFor(env1, Leader), t)

	totalApplied := 0
	applyAndWait := func(leader *RaftEnv, n int, sz int) {
		// Do some commits
		var futures []ApplyFuture
		for i := 0; i < n; i++ {
			futures = append(futures, leader.raft.Apply(logBytes(i, sz), 0))
		}
		for _, f := range futures {
			NoErr(WaitFuture(f, t), t)
			leader.logger.Printf("[DEBUG] Applied at %d, size %d", f.Index(), sz)
		}
		totalApplied += n
	}
	// Do some commits
	applyAndWait(env1, 100, 10)

	// Do a snapshot
	NoErr(WaitFuture(env1.raft.Snapshot(), t), t)

	// Join a few nodes!
	var envs []*RaftEnv
	for i := 0; i < 4; i++ {
		env := MakeRaft(t, conf)
		addr := env.trans.LocalAddr()
		NoErr(WaitFuture(env1.raft.AddPeer(addr), t), t)
		envs = append(envs, env)
	}

	// Wait for a leader
	leader, err := WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
	NoErr(err, t)

	// Do some more commits
	applyAndWait(leader, 100, 10)

	// snapshot the leader
	NoErr(WaitFuture(leader.raft.Snapshot(), t), t)

	CheckConsistent(append([]*RaftEnv{env1}, envs...), t)

	// shutdown a follower
	disconnected := envs[len(envs)-1]
	disconnected.Shutdown()

	// Do some more commits [make sure the resulting snapshot will be a reasonable size]
	applyAndWait(leader, 100, 10000)

	// snapshot the leader [leaders log should be compacted past the disconnected follower log now]
	NoErr(WaitFuture(leader.raft.Snapshot(), t), t)

	// Unfortuantly we need to wait for the leader to start backing off RPCs to the down follower
	// such that when the follower comes back up it'll run an election before it gets an rpc from
	// the leader
	time.Sleep(time.Second * 5)

	// start the now out of date follower back up
	disconnected.Restart(t)

	// wait for it to get caught up
	timeout := time.Now().Add(time.Second * 10)
	for disconnected.raft.getLastApplied() < leader.raft.getLastApplied() {
		time.Sleep(time.Millisecond)
		if time.Now().After(timeout) {
			t.Fatalf("Gave up waiting for follower to get caught up to leader")
		}
	}

	CheckConsistent(append([]*RaftEnv{env1}, envs...), t)

	// Shoot two nodes in the head!
	rm1, rm2 := envs[0], envs[1]
	rm1.Release()
	rm2.Release()
	envs = envs[2:]
	time.Sleep(10 * time.Millisecond)

	// Wait for a leader
	leader, err = WaitForAny(Leader, append([]*RaftEnv{env1}, envs...))
	NoErr(err, t)

	// Do some more commits
	applyAndWait(leader, 100, 10)

	// Join a few new nodes!
	for i := 0; i < 2; i++ {
		env := MakeRaft(t, conf)
		addr := env.trans.LocalAddr()
		NoErr(WaitFuture(leader.raft.AddPeer(addr), t), t)
		envs = append(envs, env)
	}

	// Remove the old nodes
	NoErr(WaitFuture(leader.raft.RemovePeer(rm1.raft.localAddr), t), t)
	NoErr(WaitFuture(leader.raft.RemovePeer(rm2.raft.localAddr), t), t)

	// Shoot the leader
	env1.Release()
	time.Sleep(3 * conf.HeartbeatTimeout)

	// Wait for a leader
	leader, err = WaitForAny(Leader, envs)
	NoErr(err, t)

	allEnvs := append([]*RaftEnv{env1}, envs...)
	CheckConsistent(allEnvs, t)

	if len(env1.fsm.logs) != totalApplied {
		t.Fatalf("should apply %d logs! %d", totalApplied, len(env1.fsm.logs))
	}

	for _, e := range envs {
		e.Release()
	}
}
