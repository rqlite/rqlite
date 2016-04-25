package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockListener()
	ms := &mockStore{}
	s := NewService(ml, ms)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_SetAPIPeer(t *testing.T) {
	raftAddr, apiAddr := "localhost:4002", "localhost:4001"

	s, _, ms := mustNewOpenService()
	defer s.Close()
	if err := s.SetPeer(raftAddr, apiAddr); err != nil {
		t.Fatalf("failed to set peer: %s", err.Error())
	}

	if ms.peers[raftAddr] != apiAddr {
		t.Fatalf("peer not set correctly, exp %s, got %s", apiAddr, ms.peers[raftAddr])
	}
}

func Test_SerAPIPeerNetwork(t *testing.T) {
	t.Skip("remote service not responding correctly")

	raftAddr, apiAddr := "localhost:4002", "localhost:4001"

	s, _, ms := mustNewOpenService()
	defer s.Close()

	raddr, err := net.ResolveTCPAddr("tcp", s.Addr())
	if err != nil {
		t.Fatalf("failed to resolve remote uster ervice address: %s", err.Error())
	}

	conn, err := net.DialTCP("tcp4", nil, raddr)
	if err != nil {
		t.Fatalf("failed to connect to remote cluster service: %s", err.Error())
	}
	conn.Write([]byte(fmt.Sprintf(`{"%s": "%s"}`, raftAddr, apiAddr)))
	if err != nil {
		t.Fatalf("failed to write to remote cluster service: %s", err.Error())
	}
	// XXX Check response

	if ms.peers[raftAddr] != apiAddr {
		t.Fatalf("peer not set correctly, exp %s, got %s", apiAddr, ms.peers[raftAddr])
	}
}

func mustNewOpenService() (*Service, *mockListener, *mockStore) {
	ml := mustNewMockListener()
	ms := newMockStore()
	s := NewService(ml, ms)
	if err := s.Open(); err != nil {
		panic("failed to open new service")
	}
	return s, ml, ms
}

type mockListener struct {
	ln net.Listener
}

func mustNewMockListener() *mockListener {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}
	return &mockListener{
		ln: ln,
	}
}

func (ml *mockListener) Accept() (c net.Conn, err error) {
	return ml.ln.Accept()
}

func (ml *mockListener) Addr() net.Addr {
	return ml.ln.Addr()
}

func (ml *mockListener) Close() (err error) {
	return ml.ln.Close()
}

func (ml *mockListener) Dial(addr string, t time.Duration) (net.Conn, error) {
	return nil, nil
}

type mockStore struct {
	leader string
	peers  map[string]string
}

func newMockStore() *mockStore {
	return &mockStore{
		peers: make(map[string]string),
	}
}

func (ms *mockStore) Leader() string {
	return ms.leader
}

func (ms *mockStore) UpdateAPIPeers(peers map[string]string) error {
	for k, v := range peers {
		ms.peers[k] = v
	}
	return nil
}
