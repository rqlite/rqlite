package grpc

import (
	"fmt"
	"testing"

	"github.com/rqlite/rqlite/db"
	pb "github.com/rqlite/rqlite/grpc/proto"
	"github.com/rqlite/rqlite/store"
	grpc "google.golang.org/grpc"
)

func Test_NewService(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if s == nil {
		t.Fatalf("failed to create new service")
	}
}

func Test_OpenCloseService(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if s == nil {
		t.Fatalf("failed to create new service")
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open new service")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close new service")
	}
}

func Test_ServiceLeader(t *testing.T) {
	m := &MockStore{
		leader: "http://foo:1234",
	}
	s := New("127.0.0.1:0", m, nil)
	s.Open()
	defer s.Close()

	_ = mustGrpcClient(s.Addr())
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
	conn, err := grpc.Dial(addr)
	if err != nil {
		panic(fmt.Sprintf("failed to dial grpc connection: %s", err.Error()))
	}
	return pb.NewRqliteClient(conn)
}
