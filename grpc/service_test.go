package grpc

import (
	"fmt"
	"testing"

	"github.com/rqlite/rqlite/db"
	pb "github.com/rqlite/rqlite/grpc/proto"
	"github.com/rqlite/rqlite/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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

func Test_Exec(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m, nil)
	if err := s.Open(); err != nil {
		t.Fatal("failed to open new service")
	}
	defer s.Close()
	g := mustGrpcClient(s.Addr())

	m.setSingleResult(34, 7, 389.38, "")
	r, err := g.Exec(context.Background(),
		&pb.ExecRequest{
			Stmt: []string{"CREATE TABLE foo(id INTEGER PRIMARY KEY, name TEXT"},
		})
	if err != nil {
		t.Fatalf("failed to exec single request: %s", err.Error())
	}
	if len(r.GetResults()) != 1 ||
		r.GetResults()[0].LastInsertId != 34 ||
		r.GetResults()[0].RowsAffected != 7 ||
		r.GetResults()[0].Time != 389.38 {
		t.Fatal("incorrect result for exec single request")
	}

	m.setSingleResult(99, 1, 100.1, "")
	m.setSingleResult(100, 1, 90.3, "no error")
	r, err = g.Exec(context.Background(),
		&pb.ExecRequest{
			Stmt: []string{
				`INSERT INTO foo(id, name) VALUES(1, "fiona")`,
				`INSERT INTO foo(id, name) VALUES(2, "declan")`,
			},
		})
	if err != nil {
		t.Fatalf("failed to exec single request: %s", err.Error())
	}
	if len(r.GetResults()) != 2 ||
		r.GetResults()[0].LastInsertId != 99 ||
		r.GetResults()[0].RowsAffected != 1 ||
		r.GetResults()[0].Time != 100.1 ||
		r.GetResults()[0].Error != "" ||
		r.GetResults()[1].LastInsertId != 100 ||
		r.GetResults()[1].RowsAffected != 1 ||
		r.GetResults()[1].Time != 90.3 ||
		r.GetResults()[1].Error != "no error" {
		t.Fatal("incorrect result for exec multi request")
	}
}

type MockStore struct {
	leader  string
	results []*db.Result
	rows    []*db.Rows
}

func (m *MockStore) Execute(queries []string, timings, tx bool) ([]*db.Result, error) {
	r := m.results
	m.results = nil
	return r, nil
}

func (m *MockStore) Query(queries []string, timings, tx bool, lvl store.ConsistencyLevel) ([]*db.Rows, error) {
	return m.rows, nil
}

func (m *MockStore) Leader() string {
	return m.leader
}

func (m *MockStore) Peer(addr string) string {
	return ""
}

func (m *MockStore) setSingleResult(lastInsertId, rowsAffected int64, time float64, err string) {
	r := &db.Result{
		LastInsertID: lastInsertId,
		RowsAffected: rowsAffected,
		Time:         time,
		Error:        err,
	}
	m.results = append(m.results, r)
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
