package cluster

import (
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
}

func (ms *mockStore) Leader() string {
	return ms.leader
}

func (ms *mockStore) UpdateAPIPeers(peers map[string]string) error {
	return nil
}
