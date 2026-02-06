// Package proxy provides leader-forwarding logic for rqlite operations.
// It tries a local store operation first and, if the store returns
// ErrNotLeader, transparently forwards the request to the current
// Raft leader via the cluster interface.
package proxy

import (
	"context"
	"errors"
	"expvar"
	"io"
	"time"

	clstrPB "github.com/rqlite/rqlite/v9/cluster/proto"
	"github.com/rqlite/rqlite/v9/command/proto"
	"github.com/rqlite/rqlite/v9/store"
)

var (
	// ErrNotLeader is returned when noForward is true and the local
	// node is not the Raft leader.
	ErrNotLeader = errors.New("not leader")

	// ErrLeaderNotFound is returned when the leader address is unknown.
	ErrLeaderNotFound = errors.New("leader not found")

	// ErrUnauthorized is returned when the remote node rejects credentials.
	ErrUnauthorized = errors.New("unauthorized")
)

// stats captures stats for the HTTP service.
var stats *expvar.Map

const (
	numRemoteRemoveNode       = "remote_remove_node"
	numRemoteBackups          = "remote_backups"
	numRemoteLoads            = "remote_loads"
	numRemoteStepdowns        = "remote_stepdowns"
	numRemoteExecutions       = "remote_executions"
	numRemoteExecutionsFailed = "remote_executions_failed"
	numRemoteQueries          = "remote_queries"
	numRemoteQueriesFailed    = "remote_queries_failed"
	numRemoteRequests         = "remote_requests"
	numRemoteRequestsFailed   = "remote_requests_failed"
)

func init() {
	stats = expvar.NewMap("proxy")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numRemoteRemoveNode, 0)
	stats.Add(numRemoteBackups, 0)
	stats.Add(numRemoteLoads, 0)
	stats.Add(numRemoteStepdowns, 0)
	stats.Add(numRemoteExecutions, 0)
	stats.Add(numRemoteExecutionsFailed, 0)
	stats.Add(numRemoteQueries, 0)
	stats.Add(numRemoteQueriesFailed, 0)
	stats.Add(numRemoteRequests, 0)
	stats.Add(numRemoteRequestsFailed, 0)
}

// Store defines the local database operations needed by the proxy.
type Store interface {
	Execute(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error)
	Query(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error)
	Request(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error)
	Load(ctx context.Context, lr *proto.LoadRequest) error
	Backup(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error
	Remove(ctx context.Context, rn *proto.RemoveNodeRequest) error
	Stepdown(wait bool, id string) error
	LeaderAddr() (string, error)
}

// Cluster defines operations for forwarding requests to the leader.
type Cluster interface {
	Execute(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error)
	Query(ctx context.Context, qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) ([]*proto.QueryRows, uint64, error)
	Request(ctx context.Context, eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, uint64, error)
	Backup(ctx context.Context, br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error
	Load(ctx context.Context, lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error
	RemoveNode(ctx context.Context, rn *proto.RemoveNodeRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error
	Stepdown(sr *proto.StepdownRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error
}

// Proxy handles try-local-then-forward-to-leader logic.
type Proxy struct {
	store   Store
	cluster Cluster
}

// New returns a new Proxy instance.
func New(store Store, cluster Cluster) *Proxy {
	return &Proxy{
		store:   store,
		cluster: cluster,
	}
}

// Execute executes statements that modify the database. If the local store
// returns ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Execute(ctx context.Context, er *proto.ExecuteRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) ([]*proto.ExecuteQueryResponse, uint64, error) {

	results, raftIndex, err := p.store.Execute(ctx, er)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, 0, ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, addrErr
		}
		results, raftIndex, err = p.cluster.Execute(ctx, er, addr, creds, timeout, retries)
		if err != nil {
			return nil, 0, wrapIfUnauthorized(err)
		}
	}
	return results, raftIndex, err
}

// Query executes read-only queries. If the local store returns ErrNotLeader
// and noForward is false, the request is forwarded to the current leader.
// The ConsistencyLevel return value from Store.Query is dropped.
func (p *Proxy) Query(ctx context.Context, qr *proto.QueryRequest, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) ([]*proto.QueryRows, uint64, error) {

	results, _, raftIndex, err := p.store.Query(ctx, qr)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, 0, ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, addrErr
		}
		results, raftIndex, err = p.cluster.Query(ctx, qr, addr, creds, timeout)
		if err != nil {
			return nil, 0, wrapIfUnauthorized(err)
		}
	}
	return results, raftIndex, err
}

// Request processes a unified execute-query request. If the local store
// returns ErrNotLeader and noForward is false, the request is forwarded
// to the current leader.
func (p *Proxy) Request(ctx context.Context, eqr *proto.ExecuteQueryRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {

	results, seq, raftIndex, err := p.store.Request(ctx, eqr)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return nil, 0, 0, ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return nil, 0, 0, addrErr
		}
		results, seq, raftIndex, err = p.cluster.Request(ctx, eqr, addr, creds, timeout, retries)
		if err != nil {
			return nil, 0, 0, wrapIfUnauthorized(err)
		}
	}
	return results, seq, raftIndex, err
}

// Backup writes a backup of the database. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Backup(ctx context.Context, br *proto.BackupRequest, dst io.Writer, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) error {

	err := p.store.Backup(ctx, br, dst)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return addrErr
		}
		err = p.cluster.Backup(ctx, br, addr, creds, timeout, dst)
		if err != nil {
			return wrapIfUnauthorized(err)
		}
	}
	return err
}

// Load loads a SQLite file into the cluster. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Load(ctx context.Context, lr *proto.LoadRequest, creds *clstrPB.Credentials,
	timeout time.Duration, retries int, noForward bool) error {

	err := p.store.Load(ctx, lr)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return addrErr
		}
		err = p.cluster.Load(ctx, lr, addr, creds, timeout, retries)
		if err != nil {
			return wrapIfUnauthorized(err)
		}
	}
	return err
}

// Remove removes a node from the cluster. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Remove(ctx context.Context, rn *proto.RemoveNodeRequest, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) error {

	err := p.store.Remove(ctx, rn)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return addrErr
		}
		err = p.cluster.RemoveNode(ctx, rn, addr, creds, timeout)
		if err != nil {
			return wrapIfUnauthorized(err)
		}
	}
	return err
}

// Stepdown triggers leader stepdown. If the local store returns
// ErrNotLeader and noForward is false, the request is forwarded to
// the current leader.
func (p *Proxy) Stepdown(wait bool, id string, creds *clstrPB.Credentials,
	timeout time.Duration, noForward bool) error {

	err := p.store.Stepdown(wait, id)
	if errors.Is(err, store.ErrNotLeader) {
		if noForward {
			return ErrNotLeader
		}
		addr, addrErr := p.leaderAddr()
		if addrErr != nil {
			return addrErr
		}
		sr := &proto.StepdownRequest{
			Id:   id,
			Wait: wait,
		}
		err = p.cluster.Stepdown(sr, addr, creds, timeout)
		if err != nil {
			return wrapIfUnauthorized(err)
		}
	}
	return err
}

// leaderAddr returns the Raft address of the current leader. Returns
// ErrLeaderNotFound if the address is empty.
func (p *Proxy) leaderAddr() (string, error) {
	addr, err := p.store.LeaderAddr()
	if err != nil {
		return "", err
	}
	if addr == "" {
		return "", ErrLeaderNotFound
	}
	return addr, nil
}

// wrapIfUnauthorized wraps an error as ErrUnauthorized if the error
// message is "unauthorized", allowing callers to use errors.Is().
func wrapIfUnauthorized(err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == "unauthorized" {
		return ErrUnauthorized
	}
	return err
}
