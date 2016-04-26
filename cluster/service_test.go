package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
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

func Test_SetAPIPeerNetwork(t *testing.T) {
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
	if _, err := conn.Write([]byte(fmt.Sprintf(`{"%s": "%s"}`, raftAddr, apiAddr))); err != nil {
		t.Fatalf("failed to write to remote cluster service: %s", err.Error())
	}

	resp := response{}
	d := json.NewDecoder(conn)
	err = d.Decode(&resp)
	if err != nil {
		t.Fatalf("failed to decode response: %s", err.Error())
	}

	if resp.Code != 0 {
		t.Fatalf("response code was non-zero")
	}

	if ms.peers[raftAddr] != apiAddr {
		t.Fatalf("peer not set correctly, exp %s, got %s", apiAddr, ms.peers[raftAddr])
	}
}

func Test_SetAPIPeerFailUpdate(t *testing.T) {
	raftAddr, apiAddr := "localhost:4002", "localhost:4001"

	s, _, ms := mustNewOpenService()
	defer s.Close()
	ms.failUpdateAPIPeers = true

	// Attempt to set peer without a leader
	if err := s.SetPeer(raftAddr, apiAddr); err == nil {
		t.Fatalf("no error returned by set peer when no leader")
	}

	// Start a network server.
	tn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to open test server: %s", err.Error())
	}
	ms.leader = tn.Addr().String()

	c := make(chan map[string]string, 1)
	go func() {
		conn, err := tn.Accept()
		if err != nil {
			t.Fatalf("failed to accept connection from cluster: %s", err.Error())
		}
		t.Logf("test server received connection from: %s", conn.RemoteAddr())

		peers := make(map[string]string)
		d := json.NewDecoder(conn)
		err = d.Decode(&peers)
		if err != nil {
			t.Fatalf("failed to decode message from cluster: %s", err.Error())
		}

		// Response OK.
		// Let the remote node know everything went OK.
		if _, err := conn.Write(respOKMarshalled); err != nil {
			t.Fatalf("failed to respond to cluster: %s", err.Error())
		}

		c <- peers
	}()

	if err := s.SetPeer(raftAddr, apiAddr); err != nil {
		t.Fatalf("failed to set peer on cluster: %s", err.Error())
	}

	peers := <-c
	if peers[raftAddr] != apiAddr {
		t.Fatalf("peer not set correctly, exp %s, got %s", apiAddr, ms.peers[raftAddr])
	}
}

func mustNewOpenService() (*Service, *mockTransport, *mockStore) {
	ml := mustNewMockTransport()
	ms := newMockStore()
	s := NewService(ml, ms)
	if err := s.Open(); err != nil {
		panic("failed to open new service")
	}
	return s, ml, ms
}

type mockTransport struct {
	tn net.Listener
}

func mustNewMockTransport() *mockTransport {
	tn, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create mock listener")
	}
	return &mockTransport{
		tn: tn,
	}
}

func (ml *mockTransport) Accept() (c net.Conn, err error) {
	return ml.tn.Accept()
}

func (ml *mockTransport) Addr() net.Addr {
	return ml.tn.Addr()
}

func (ml *mockTransport) Close() (err error) {
	return ml.tn.Close()
}

func (ml *mockTransport) Dial(addr string, t time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 5*time.Second)
}

type mockStore struct {
	leader             string
	peers              map[string]string
	failUpdateAPIPeers bool
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
	if ms.failUpdateAPIPeers {
		return fmt.Errorf("forced fail")
	}

	for k, v := range peers {
		ms.peers[k] = v
	}
	return nil
}
