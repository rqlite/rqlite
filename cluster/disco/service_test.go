package disco

import (
	"sync"
	"testing"
	"time"
)

func Test_NewService(t *testing.T) {
	s := NewService(&mockClient{}, &mockStore{}, Voter)
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
	c := &mockStore{}

	s := NewService(m, c, Voter)
	s.RegisterInterval = 10 * time.Millisecond

	ok, addr, err := s.Register("1", "localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error registering with disco: %s", err.Error())
	}
	if ok {
		t.Fatalf("registered as leader unexpectedly")
	}
	if exp, got := "localhost:4004", addr; exp != got {
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
	c := &mockStore{}

	s := NewService(m, c, Voter)
	s.RegisterInterval = 10 * time.Millisecond

	ok, addr, err := s.Register("1", "localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error registering with disco: %s", err.Error())
	}
	if !ok {
		t.Fatalf("failed to register as expected")
	}
	if exp, got := "localhost:4002", addr; exp != got {
		t.Fatalf("returned addressed incorrect, exp %s, got %s", exp, got)
	}
}

func Test_RegisterNonVoter(t *testing.T) {
	m := &mockClient{}
	getCalled := false
	m.getLeaderFn = func() (id string, apiAddr string, addr string, ok bool, e error) {
		if getCalled {
			return "2", "localhost:4003", "localhost:4004", true, nil
		}
		getCalled = true
		return "", "", "", false, nil
	}
	m.initializeLeaderFn = func(tID, tAPIAddr, tAddr string) (bool, error) {
		t.Fatal("InitializeLeader called unexpectedly")
		return false, nil
	}

	c := &mockStore{}
	s := NewService(m, c, NonVoter)
	s.RegisterInterval = 10 * time.Millisecond

	ok, addr, err := s.Register("1", "localhost:4001", "localhost:4002")
	if err != nil {
		t.Fatalf("error registering with disco: %s", err.Error())
	}
	if ok {
		t.Fatalf("registered incorrectly as non-voter")
	}
	if exp, got := "localhost:4004", addr; exp != got {
		t.Fatalf("returned addressed incorrect, exp %s, got %s", exp, got)
	}
}

func Test_StartReportingChange(t *testing.T) {
	var wg sync.WaitGroup
	m := &mockClient{}
	m.setLeaderFn = func(id, apiAddr, addr string) error {
		defer wg.Done()
		if id != "1" || apiAddr != "localhost:4001" || addr != "localhost:4002" {
			t.Fatalf("wrong values passed to SetLeader")
		}
		return nil
	}
	c := &mockStore{}
	c.registerLeaderChangeFn = func(c chan<- bool) {
		c <- true
	}

	wg.Add(1)
	s := NewService(m, c, Voter)
	s.ReportInterval = 10 * time.Minute // Nothing will happen due to timer.
	done := s.StartReporting("1", "localhost:4001", "localhost:4002")

	// Wait for leadership change
	wg.Wait()
	close(done)
}

// Test_StartReportingChange_Timer tests that once a node become the leader,
// the StartReporting function will then periodically report its leadership
// status to the discovery service, even if the leadership change channel
// does not trigger.
func Test_StartReportingChange_Timer(t *testing.T) {
	m := &mockClient{}
	ch := make(chan struct{})
	nCalled := 0
	m.setLeaderFn = func(id, apiAddr, addr string) error {
		nCalled++
		if nCalled > 1 {
			ch <- struct{}{}
		}
		if id != "1" || apiAddr != "localhost:4001" || addr != "localhost:4002" {
			t.Fatalf("wrong values passed to SetLeader")
		}
		return nil
	}
	c := &mockStore{}
	c.registerLeaderChangeFn = func(c chan<- bool) {
		c <- true
	}
	c.isLeaderFn = func() bool {
		return true // Simulate that this node is the leader.
	}

	s := NewService(m, c, Voter)
	s.ReportInterval = 100 * time.Millisecond
	done := s.StartReporting("1", "localhost:4001", "localhost:4002")

	<-ch // Wait for at least two calls to SetLeader
	close(done)
}

type mockClient struct {
	getLeaderFn        func() (id string, apiAddr string, addr string, ok bool, e error)
	initializeLeaderFn func(id, apiAddr, addr string) (bool, error)
	setLeaderFn        func(id, apiAddr, addr string) error
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
	if m.setLeaderFn != nil {
		return m.setLeaderFn(id, apiAddr, addr)
	}
	return nil
}

func (m *mockClient) String() string {
	return "mock"
}

type mockStore struct {
	isLeaderFn             func() bool
	registerLeaderChangeFn func(c chan<- bool)
}

func (m *mockStore) IsLeader() bool {
	if m.isLeaderFn != nil {
		return m.isLeaderFn()
	}
	return false
}

func (m *mockStore) RegisterLeaderChange(c chan<- bool) {
	if m.registerLeaderChangeFn != nil {
		m.registerLeaderChangeFn(c)
	}
}
