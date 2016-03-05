package http

import (
	"fmt"
	"net/http"
	"testing"

	sql "github.com/otoolep/rqlite/db"
)

func Test_NewService(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m)
	if s == nil {
		t.Fatalf("failed to create new service")
	}
}

func Test_404Routes(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/xxx")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 404 {
		t.Fatalf("failed to get expected 404, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/xxx", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 404 {
		t.Fatalf("failed to get expected 404, got %d", resp.StatusCode)
	}
}

func Test_405Routes(t *testing.T) {
	m := &MockStore{}
	s := New("127.0.0.1:0", m)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start service")
	}
	defer s.Close()
	host := fmt.Sprintf("http://%s", s.Addr().String())

	client := &http.Client{}

	resp, err := client.Get(host + "/db/execute")
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/db/backup", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}

	resp, err = client.Post(host+"/status", "", nil)
	if err != nil {
		t.Fatalf("failed to make request")
	}
	if resp.StatusCode != 405 {
		t.Fatalf("failed to get expected 405, got %d", resp.StatusCode)
	}
}

type MockStore struct {
}

func (m *MockStore) Execute(queries []string, tx bool) ([]*sql.Result, error) {
	return nil, nil
}

func (m *MockStore) Query(queries []string, tx, leader, verify bool) ([]*sql.Rows, error) {
	return nil, nil
}

func (m *MockStore) Join(addr string) error {
	return nil
}

func (m * MockStore) Leader() string {
	return ""
}

func (m *MockStore) Stats() (map[string]interface{}, error) {
	return nil, nil
}

func (m *MockStore) Backup(leader bool) ([]byte, error) {
	return nil, nil
}
