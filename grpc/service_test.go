package grpc

import (
	"fmt"
	"testing"

	"github.com/rqlite/rqlite/db"
	pb "github.com/rqlite/rqlite/grpc/proto"
	"github.com/rqlite/rqlite/store"
	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

func Test_NewService(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if s == nil {
		t.Fatal("failed to create new service")
	}
}

func Test_OpenCloseService(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if s == nil {
		t.Fatal("failed to create new service")
	}
	if err := s.Open(); err != nil {
		t.Fatal("failed to open new service")
	}
	if err := s.Close(); err != nil {
		t.Fatal("failed to close new service")
	}
}

func Test_ServiceLeader(t *testing.T) {
	exp := "http://foo:1234"
	m := &MockStore{
		leader: exp,
	}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Open(); err != nil {
		t.Fatal("failed to open new service")
	}
	defer s.Close()

	g := mustGrpcClient(s.Addr())
	r, err := g.Leader(context.Background(), &pb.LeaderRequest{})
	if err != nil {
		t.Fatalf("failed to retrieve leader: %s", err.Error())
	}
	if got := r.GetLeader(); got != exp {
		t.Fatalf("incorrect leader received: %s, expected: %s", got, exp)
	}
}

func Test_ExecSingle(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Open(); err != nil {
		t.Fatal("failed to open new service")
	}
	defer s.Close()

	g := mustGrpcClient(s.Addr())
	_, err := g.Exec(context.Background(),
		&pb.ExecRequest{
			Stmt: []string{"CREATE TABLE foo(id INTEGER PRIMARY KEY, name TEXT"},
		})
	if err != nil {
		t.Fatalf("failed to exec single request: %s", err.Error())
	}
}

type MockStore struct {
	leader    string
	executeFn func(queries []string, tx bool) ([]*db.Result, error)
	queryFn   func(queries []string, tx, leader, verify bool) ([]*db.Rows, error)
}

func (m *MockStore) Execute(queries []string, timings, tx bool) ([]*db.Result, error) {
	if m.executeFn == nil {
		return nil, nil
	}
	return nil, nil
}

func (m *MockStore) Query(queries []string, timings, tx bool, lvl store.ConsistencyLevel) ([]*db.Rows, error) {
	if m.queryFn == nil {
		return nil, nil
	}
	return nil, nil
}

func (m *MockStore) Leader() string {
	return m.leader
}

func (m *MockStore) Peer(addr string) string {
	return ""
}

type mockCredentialStore struct {
	CheckOK   bool
	HasPermOK bool
}

func (m *mockCredentialStore) Check(username, password string) bool {
	return m.CheckOK
}

func (m *mockCredentialStore) HasPerm(username, perm string) bool {
	return m.HasPermOK
}

func mustGrpcClient(addr string) pb.RqliteClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(fmt.Sprintf("failed to dial grpc connection: %s", err.Error()))
	}
	return pb.NewRqliteClient(conn)
}
