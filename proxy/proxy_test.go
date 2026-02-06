package proxy

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	clstrPB "github.com/rqlite/rqlite/v9/cluster/proto"
	"github.com/rqlite/rqlite/v9/command/proto"
	"github.com/rqlite/rqlite/v9/store"
)

// mockStore implements Store for testing.
type mockStore struct {
	executeFn    func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error)
	queryFn      func(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error)
	requestFn    func(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error)
	loadFn       func(ctx context.Context, lr *proto.LoadRequest) error
	backupFn     func(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error
	removeFn     func(ctx context.Context, rn *proto.RemoveNodeRequest) error
	stepdownFn   func(wait bool, id string) error
	leaderAddrFn func() (string, error)
}

func (m *mockStore) Execute(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
	if m.executeFn != nil {
		return m.executeFn(ctx, er)
	}
	return nil, 0, nil
}

func (m *mockStore) Query(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, qr)
	}
	return nil, proto.ConsistencyLevel_NONE, 0, nil
}

func (m *mockStore) Request(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
	if m.requestFn != nil {
		return m.requestFn(ctx, eqr)
	}
	return nil, 0, 0, nil
}

func (m *mockStore) Load(ctx context.Context, lr *proto.LoadRequest) error {
	if m.loadFn != nil {
		return m.loadFn(ctx, lr)
	}
	return nil
}

func (m *mockStore) Backup(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error {
	if m.backupFn != nil {
		return m.backupFn(ctx, br, dst)
	}
	return nil
}

func (m *mockStore) Remove(ctx context.Context, rn *proto.RemoveNodeRequest) error {
	if m.removeFn != nil {
		return m.removeFn(ctx, rn)
	}
	return nil
}

func (m *mockStore) Stepdown(wait bool, id string) error {
	if m.stepdownFn != nil {
		return m.stepdownFn(wait, id)
	}
	return nil
}

func (m *mockStore) LeaderAddr() (string, error) {
	if m.leaderAddrFn != nil {
		return m.leaderAddrFn()
	}
	return "", nil
}

// mockCluster implements Cluster for testing.
type mockCluster struct {
	executeFn    func(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error)
	queryFn      func(ctx context.Context, qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) ([]*proto.QueryRows, uint64, error)
	requestFn    func(ctx context.Context, eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, uint64, error)
	backupFn     func(ctx context.Context, br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error
	loadFn       func(ctx context.Context, lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error
	removeNodeFn func(ctx context.Context, rn *proto.RemoveNodeRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error
	stepdownFn   func(sr *proto.StepdownRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error
}

func (m *mockCluster) Execute(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error) {
	if m.executeFn != nil {
		return m.executeFn(ctx, er, nodeAddr, creds, timeout, retries)
	}
	return nil, 0, nil
}

func (m *mockCluster) Query(ctx context.Context, qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) ([]*proto.QueryRows, uint64, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, qr, nodeAddr, creds, timeout)
	}
	return nil, 0, nil
}

func (m *mockCluster) Request(ctx context.Context, eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
	if m.requestFn != nil {
		return m.requestFn(ctx, eqr, nodeAddr, creds, timeout, retries)
	}
	return nil, 0, 0, nil
}

func (m *mockCluster) Backup(ctx context.Context, br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error {
	if m.backupFn != nil {
		return m.backupFn(ctx, br, nodeAddr, creds, timeout, w)
	}
	return nil
}

func (m *mockCluster) Load(ctx context.Context, lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error {
	if m.loadFn != nil {
		return m.loadFn(ctx, lr, nodeAddr, creds, timeout, retries)
	}
	return nil
}

func (m *mockCluster) RemoveNode(ctx context.Context, rn *proto.RemoveNodeRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error {
	if m.removeNodeFn != nil {
		return m.removeNodeFn(ctx, rn, nodeAddr, creds, timeout)
	}
	return nil
}

func (m *mockCluster) Stepdown(sr *proto.StepdownRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error {
	if m.stepdownFn != nil {
		return m.stepdownFn(sr, nodeAddr, creds, timeout)
	}
	return nil
}

func newTestProxy(s Store, c Cluster) *Proxy {
	return New(s, c)
}

func Test_Execute_LocalSuccess(t *testing.T) {
	t.Parallel()
	expected := []*proto.ExecuteQueryResponse{{}}
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return expected, 42, nil
		},
	}
	p := newTestProxy(s, &mockCluster{})

	results, idx, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if idx != 42 {
		t.Fatalf("expected raft index 42, got %d", idx)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Execute_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	_, _, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Execute_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	expected := []*proto.ExecuteQueryResponse{{}}
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error) {
			if nodeAddr != "leader:4002" {
				t.Fatalf("expected forwarding to leader:4002, got %s", nodeAddr)
			}
			return expected, 99, nil
		},
	}
	p := newTestProxy(s, c)

	results, idx, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if idx != 99 {
		t.Fatalf("expected raft index 99, got %d", idx)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Execute_LeaderNotFound(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "", nil
		},
	}
	p := newTestProxy(s, &mockCluster{})

	_, _, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if !errors.Is(err, ErrLeaderNotFound) {
		t.Fatalf("expected ErrLeaderNotFound, got %v", err)
	}
}

func Test_Execute_Unauthorized(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, errors.New("unauthorized")
		},
	}
	p := newTestProxy(s, c)

	_, _, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func Test_Execute_ClusterOtherError(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, errors.New("some other error")
		},
	}
	p := newTestProxy(s, c)

	_, _, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if err == nil || err.Error() != "some other error" {
		t.Fatalf("expected 'some other error', got %v", err)
	}
}

func Test_Execute_StoreOtherError(t *testing.T) {
	t.Parallel()
	storeErr := errors.New("database locked")
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, storeErr
		},
	}
	p := newTestProxy(s, &mockCluster{})

	_, _, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if err != storeErr {
		t.Fatalf("expected store error, got %v", err)
	}
}

func Test_Query_LocalSuccess(t *testing.T) {
	t.Parallel()
	expected := []*proto.QueryRows{{}}
	s := &mockStore{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error) {
			return expected, proto.ConsistencyLevel_WEAK, 10, nil
		},
	}
	p := newTestProxy(s, &mockCluster{})

	results, idx, err := p.Query(context.Background(), &proto.QueryRequest{}, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if idx != 10 {
		t.Fatalf("expected raft index 10, got %d", idx)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Query_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error) {
			return nil, proto.ConsistencyLevel_NONE, 0, store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	_, _, err := p.Query(context.Background(), &proto.QueryRequest{}, nil, time.Second, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Query_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	expected := []*proto.QueryRows{{}}
	s := &mockStore{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error) {
			return nil, proto.ConsistencyLevel_NONE, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) ([]*proto.QueryRows, uint64, error) {
			if nodeAddr != "leader:4002" {
				t.Fatalf("expected forwarding to leader:4002, got %s", nodeAddr)
			}
			return expected, 20, nil
		},
	}
	p := newTestProxy(s, c)

	results, idx, err := p.Query(context.Background(), &proto.QueryRequest{}, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if idx != 20 {
		t.Fatalf("expected raft index 20, got %d", idx)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Query_Unauthorized(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest) ([]*proto.QueryRows, proto.ConsistencyLevel, uint64, error) {
			return nil, proto.ConsistencyLevel_NONE, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		queryFn: func(ctx context.Context, qr *proto.QueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) ([]*proto.QueryRows, uint64, error) {
			return nil, 0, errors.New("unauthorized")
		},
	}
	p := newTestProxy(s, c)

	_, _, err := p.Query(context.Background(), &proto.QueryRequest{}, nil, time.Second, false)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func Test_Request_LocalSuccess(t *testing.T) {
	t.Parallel()
	expected := []*proto.ExecuteQueryResponse{{}}
	s := &mockStore{
		requestFn: func(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
			return expected, 5, 50, nil
		},
	}
	p := newTestProxy(s, &mockCluster{})

	results, seq, idx, err := p.Request(context.Background(), &proto.ExecuteQueryRequest{}, nil, time.Second, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if seq != 5 {
		t.Fatalf("expected seq 5, got %d", seq)
	}
	if idx != 50 {
		t.Fatalf("expected raft index 50, got %d", idx)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Request_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		requestFn: func(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
			return nil, 0, 0, store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	_, _, _, err := p.Request(context.Background(), &proto.ExecuteQueryRequest{}, nil, time.Second, 0, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Request_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	expected := []*proto.ExecuteQueryResponse{{}}
	s := &mockStore{
		requestFn: func(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
			return nil, 0, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		requestFn: func(ctx context.Context, eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
			return expected, 7, 70, nil
		},
	}
	p := newTestProxy(s, c)

	results, seq, idx, err := p.Request(context.Background(), &proto.ExecuteQueryRequest{}, nil, time.Second, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if seq != 7 {
		t.Fatalf("expected seq 7, got %d", seq)
	}
	if idx != 70 {
		t.Fatalf("expected raft index 70, got %d", idx)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}

func Test_Request_Unauthorized(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		requestFn: func(ctx context.Context, eqr *proto.ExecuteQueryRequest) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
			return nil, 0, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		requestFn: func(ctx context.Context, eqr *proto.ExecuteQueryRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) ([]*proto.ExecuteQueryResponse, uint64, uint64, error) {
			return nil, 0, 0, errors.New("unauthorized")
		},
	}
	p := newTestProxy(s, c)

	_, _, _, err := p.Request(context.Background(), &proto.ExecuteQueryRequest{}, nil, time.Second, 0, false)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func Test_Backup_LocalSuccess(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		backupFn: func(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error {
			return nil
		},
	}
	p := newTestProxy(s, &mockCluster{})

	err := p.Backup(context.Background(), &proto.BackupRequest{}, nil, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Backup_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		backupFn: func(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error {
			return store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	err := p.Backup(context.Background(), &proto.BackupRequest{}, nil, nil, time.Second, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Backup_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		backupFn: func(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		backupFn: func(ctx context.Context, br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error {
			if nodeAddr != "leader:4002" {
				t.Fatalf("expected forwarding to leader:4002, got %s", nodeAddr)
			}
			return nil
		},
	}
	p := newTestProxy(s, c)

	err := p.Backup(context.Background(), &proto.BackupRequest{}, nil, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Backup_Unauthorized(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		backupFn: func(ctx context.Context, br *proto.BackupRequest, dst io.Writer) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		backupFn: func(ctx context.Context, br *proto.BackupRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, w io.Writer) error {
			return errors.New("unauthorized")
		},
	}
	p := newTestProxy(s, c)

	err := p.Backup(context.Background(), &proto.BackupRequest{}, nil, nil, time.Second, false)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func Test_Load_LocalSuccess(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		loadFn: func(ctx context.Context, lr *proto.LoadRequest) error {
			return nil
		},
	}
	p := newTestProxy(s, &mockCluster{})

	err := p.Load(context.Background(), &proto.LoadRequest{}, nil, time.Second, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Load_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		loadFn: func(ctx context.Context, lr *proto.LoadRequest) error {
			return store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	err := p.Load(context.Background(), &proto.LoadRequest{}, nil, time.Second, 0, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Load_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		loadFn: func(ctx context.Context, lr *proto.LoadRequest) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		loadFn: func(ctx context.Context, lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error {
			if nodeAddr != "leader:4002" {
				t.Fatalf("expected forwarding to leader:4002, got %s", nodeAddr)
			}
			return nil
		},
	}
	p := newTestProxy(s, c)

	err := p.Load(context.Background(), &proto.LoadRequest{}, nil, time.Second, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Load_Unauthorized(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		loadFn: func(ctx context.Context, lr *proto.LoadRequest) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		loadFn: func(ctx context.Context, lr *proto.LoadRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration, retries int) error {
			return errors.New("unauthorized")
		},
	}
	p := newTestProxy(s, c)

	err := p.Load(context.Background(), &proto.LoadRequest{}, nil, time.Second, 0, false)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func Test_Remove_LocalSuccess(t *testing.T) {
	t.Parallel()
	s := &mockStore{}
	p := newTestProxy(s, &mockCluster{})

	err := p.Remove(context.Background(), &proto.RemoveNodeRequest{Id: "node1"}, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Remove_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		removeFn: func(ctx context.Context, rn *proto.RemoveNodeRequest) error {
			return store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	err := p.Remove(context.Background(), &proto.RemoveNodeRequest{Id: "node1"}, nil, time.Second, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Remove_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		removeFn: func(ctx context.Context, rn *proto.RemoveNodeRequest) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		removeNodeFn: func(ctx context.Context, rn *proto.RemoveNodeRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error {
			if nodeAddr != "leader:4002" {
				t.Fatalf("expected forwarding to leader:4002, got %s", nodeAddr)
			}
			return nil
		},
	}
	p := newTestProxy(s, c)

	err := p.Remove(context.Background(), &proto.RemoveNodeRequest{Id: "node1"}, nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Stepdown_LocalSuccess(t *testing.T) {
	t.Parallel()
	s := &mockStore{}
	p := newTestProxy(s, &mockCluster{})

	err := p.Stepdown(true, "node1", nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Stepdown_NotLeader_NoForward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		stepdownFn: func(wait bool, id string) error {
			return store.ErrNotLeader
		},
	}
	p := newTestProxy(s, &mockCluster{})

	err := p.Stepdown(true, "", nil, time.Second, true)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func Test_Stepdown_NotLeader_Forward(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		stepdownFn: func(wait bool, id string) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		stepdownFn: func(sr *proto.StepdownRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error {
			if nodeAddr != "leader:4002" {
				t.Fatalf("expected forwarding to leader:4002, got %s", nodeAddr)
			}
			if !sr.Wait {
				t.Fatal("expected Wait to be true")
			}
			if sr.Id != "node1" {
				t.Fatalf("expected Id 'node1', got '%s'", sr.Id)
			}
			return nil
		},
	}
	p := newTestProxy(s, c)

	err := p.Stepdown(true, "node1", nil, time.Second, false)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func Test_Stepdown_Unauthorized(t *testing.T) {
	t.Parallel()
	s := &mockStore{
		stepdownFn: func(wait bool, id string) error {
			return store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "leader:4002", nil
		},
	}
	c := &mockCluster{
		stepdownFn: func(sr *proto.StepdownRequest, nodeAddr string, creds *clstrPB.Credentials, timeout time.Duration) error {
			return errors.New("unauthorized")
		},
	}
	p := newTestProxy(s, c)

	err := p.Stepdown(true, "", nil, time.Second, false)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func Test_LeaderAddrError(t *testing.T) {
	t.Parallel()
	leaderErr := errors.New("raft error")
	s := &mockStore{
		executeFn: func(ctx context.Context, er *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, uint64, error) {
			return nil, 0, store.ErrNotLeader
		},
		leaderAddrFn: func() (string, error) {
			return "", leaderErr
		},
	}
	p := newTestProxy(s, &mockCluster{})

	_, _, err := p.Execute(context.Background(), &proto.ExecuteRequest{}, nil, time.Second, 0, false)
	if err != leaderErr {
		t.Fatalf("expected leader error, got %v", err)
	}
}
