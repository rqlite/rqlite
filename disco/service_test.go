package disco

import (
	"testing"
	"time"
)

func Test_NewServce(t *testing.T) {
	s := NewService(&mockClient{})
	if s == nil {
		t.Fatalf("service is nil")
	}
}

func Test_RegisterGetLeaderOK(t *testing.T) {
	m := &mockClient{}
	m.getLeaderFn = func() (id string, apiAddr string, addr string, ok bool, e error) {
		return "2", "localhost:4003", "localhost:4004", true, nil
	}
	m.initializeLeaderFn = func(tID, tAPIAddr, tAddr string) (bool, error) {
		t.Fatalf("Leader initialized unexpectedly")
		return false, nil
	}

	s := NewService(m)
	s.UpdateInterval = 10 * time.Millisecond

	ok, addr, err := s.Register("1", "localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error registering with disco: %s", err.Error())
	}
	if ok {
		t.Fatalf("registered as leader unexpectedly")
	}
	if exp, got := "localhost:4003", addr; exp != got {
		t.Fatalf("returned addressed incorrect, exp %s, got %s", exp, got)
	}
}

func Test_RegisterInitializeLeader(t *testing.T) {
	m := &mockClient{}
	m.getLeaderFn = func() (id string, apiAddr string, addr string, ok bool, e error) {
		return "", "", "", false, nil
	}
	m.initializeLeaderFn = func(tID, tAPIAddr, tAddr string) (bool, error) {
		if tID != "1" || tAPIAddr != "localhost:4001" || tAddr != "localhost:4002" {
			t.Fatalf("wrong values passed to InitializeLeader")
		}
		return true, nil
	}

	s := NewService(m)
	s.UpdateInterval = 10 * time.Millisecond

	ok, addr, err := s.Register("1", "localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error registering with disco: %s", err.Error())
	}
	if !ok {
		t.Fatalf("failed to register as expected")
	}
	if exp, got := "localhost:4001", addr; exp != got {
		t.Fatalf("returned addressed incorrect, exp %s, got %s", exp, got)
	}
}

type mockClient struct {
	getLeaderFn        func() (id string, apiAddr string, addr string, ok bool, e error)
	initializeLeaderFn func(id, apiAddr, addr string) (bool, error)
	setLeaderFN        func(id, apiAddr, addr string) error
}

func (m *mockClient) GetLeader() (id string, apiAddr string, addr string, ok bool, e error) {
	if m.getLeaderFn != nil {
		return m.getLeaderFn()
	}
	return
}

func (m *mockClient) InitializeLeader(id, apiAddr, addr string) (bool, error) {
	if m.initializeLeaderFn != nil {
		return m.initializeLeaderFn(id, apiAddr, addr)
	}
	return false, nil
}

func (m *mockClient) SetLeader(id, apiAddr, addr string) error {
	if m.setLeaderFN != nil {
		return m.setLeaderFN(id, apiAddr, addr)
	}
	return nil
}

func (m *mockClient) String() string {
	return "mock"
}
