package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rqlite/rqlite/cluster/servicetest"
	"google.golang.org/protobuf/proto"
)

func Test_NewRemover(t *testing.T) {
	r := NewRemover(nil, 0, nil)
	if r == nil {
		t.Fatal("expected remover, got nil")
	}
}

func Test_RemoveOK(t *testing.T) {
	ch := make(chan struct{}, 1)
	defer close(ch)
	srv := makeServiceRemoveOK(t, ch)
	defer srv.Close()

	control := &mockControl{leader: srv.Addr()}
	client := NewClient(&simpleDialer{}, 0)

	r := NewRemover(client, time.Second, control)
	if err := r.Do("node-id", true); err != nil {
		t.Fatalf("failed to remove node: %s", err.Error())
	}

	waitForChan(t, ch, time.Second)
}

func Test_RemoveWaitForLeaderFail(t *testing.T) {
	srv := makeServiceRemoveOK(t, nil)
	defer srv.Close()

	control := &mockControl{
		leader:           srv.Addr(),
		waitForLeaderErr: fmt.Errorf("timeout waiting for leader"),
	}
	client := NewClient(&simpleDialer{}, 0)

	r := NewRemover(client, time.Second, control)
	if err := r.Do("node-id", true); err == nil {
		t.Fatalf("failed to detect timeout waiting for leader")
	}
}

func Test_RemoveWaitForRemoveFail(t *testing.T) {
	ch := make(chan struct{}, 1)
	defer close(ch)
	srv := makeServiceRemoveOK(t, ch)
	defer srv.Close()

	control := &mockControl{
		leader:            srv.Addr(),
		waitForRemovalErr: fmt.Errorf("timeout waiting for removal"),
	}
	client := NewClient(&simpleDialer{}, 0)

	r := NewRemover(client, time.Second, control)
	if err := r.Do("node-id", true); err == nil {
		t.Fatalf("failed to detect timeout waiting for removal")
	}

	waitForChan(t, ch, time.Second)
}

func Test_RemoveWaitForRemoveFailOK(t *testing.T) {
	ch := make(chan struct{}, 1)
	defer close(ch)
	srv := makeServiceRemoveOK(t, ch)
	defer srv.Close()

	control := &mockControl{
		leader:            srv.Addr(),
		waitForRemovalErr: fmt.Errorf("timeout waiting for removal"),
	}
	client := NewClient(&simpleDialer{}, 0)

	r := NewRemover(client, time.Second, control)
	if err := r.Do("node-id", false); err != nil {
		t.Fatalf("removal timeout not ignored: %s", err.Error())
	}

	waitForChan(t, ch, time.Second)
}

// makeServiceRemoveOK returns a service that responds to a remove node
// request with a successful response. If ch is nil, the service will
// not instantiate the handler, as the assumption is the caller does not
// expect it to be called.
func makeServiceRemoveOK(t *testing.T, ch chan struct{}) *servicetest.Service {
	srv := servicetest.NewService()
	if ch != nil {
		srv.Handler = func(conn net.Conn) {
			var p []byte
			var err error
			_ = readCommand(conn)
			p, err = proto.Marshal(&CommandRemoveNodeResponse{})
			if err != nil {
				t.Fatalf("failed to marshal response: %s", err.Error())
			}
			writeBytesWithLength(conn, p)
		}
		ch <- struct{}{}
	} else {
		srv.Handler = func(conn net.Conn) {
			t.Fatalf("unexpected call to handler")
		}
	}
	srv.Start()
	return srv
}

type mockControl struct {
	leader            string
	waitForLeaderErr  error
	waitForRemovalErr error
}

func (m *mockControl) WaitForLeader(timeout time.Duration) (string, error) {
	return m.leader, m.waitForLeaderErr
}

func (m *mockControl) WaitForRemoval(id string, timeout time.Duration) error {
	return m.waitForRemovalErr
}

func waitForChan(t *testing.T, ch chan struct{}, timeout time.Duration) {
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	select {
	case <-ch:
		return
	case <-tmr.C:
		t.Fatal("timed out waiting for channel to receive signal")
	}
}
