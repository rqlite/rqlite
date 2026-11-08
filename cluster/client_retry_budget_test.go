package cluster

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/cluster/proto"
	"github.com/rqlite/rqlite/v10/cluster/servicetest"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

// Test_ClientRetryBudget verifies the exact number of sends after transport failures.
func Test_ClientRetryBudget(t *testing.T) {
	for _, retries := range []int{-1, 0, 1, 3} {
		t.Run(fmt.Sprint(retries), func(t *testing.T) {
			var sends atomic.Int32
			srv := servicetest.NewService()
			srv.Handler = func(conn net.Conn) {
				if readCommand(conn) != nil {
					sends.Add(1)
				}
			}
			srv.Start()
			defer srv.Close()
			c := NewClient(&simpleDialer{}, time.Second)
			_, nr, err := c.retry(context.Background(), &proto.Command{Type: proto.Command_COMMAND_TYPE_QUERY}, srv.Addr(), time.Second, retries)
			if err == nil || nr != max(0, retries) || sends.Load() != int32(1+max(0, retries)) {
				t.Fatalf("sends=%d retries=%d error=%v", sends.Load(), nr, err)
			}
		})
	}
}

// Test_ClientWritesNotResent verifies ambiguous timeouts never resend at zero retries.
func Test_ClientWritesNotResent(t *testing.T) {
	for _, kind := range []string{"execute", "request", "load"} {
		t.Run(kind, func(t *testing.T) {
			var applied atomic.Int32
			release := make(chan struct{})
			srv := servicetest.NewService()
			srv.Handler = func(conn net.Conn) {
				if readCommand(conn) != nil {
					applied.Add(1)
					<-release
				}
			}
			srv.Start()
			defer srv.Close()
			defer close(release)
			c := NewClient(&simpleDialer{}, time.Second)
			var err error
			switch kind {
			case "execute":
				_, _, err = c.Execute(context.Background(), &command.ExecuteRequest{}, srv.Addr(), nil, 50*time.Millisecond, 0)
			case "request":
				_, _, _, err = c.Request(context.Background(), &command.ExecuteQueryRequest{}, srv.Addr(), nil, 50*time.Millisecond, 0)
			case "load":
				err = c.Load(context.Background(), &command.LoadRequest{}, srv.Addr(), nil, 50*time.Millisecond, 0)
			}
			if err == nil || applied.Load() != 1 || c.numForcedNewConns.Load() != 0 {
				t.Fatalf("applied=%d fresh=%d error=%v", applied.Load(), c.numForcedNewConns.Load(), err)
			}
		})
	}
}

type retryTestDialer func(string, time.Duration) (net.Conn, error)

func (d retryTestDialer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return d(addr, timeout)
}

type retryWriteFailure struct{ net.Conn }

func (*retryWriteFailure) SetDeadline(time.Time) error { return nil }
func (*retryWriteFailure) Write([]byte) (int, error)   { return 0, io.ErrClosedPipe }
func (*retryWriteFailure) Close() error                { return nil }

// Test_ClientRetryFailures verifies dial and write failures obey the same budget.
func Test_ClientRetryFailures(t *testing.T) {
	for _, phase := range []string{"dial", "write", "cancel"} {
		t.Run(phase, func(t *testing.T) {
			for _, retries := range []int{0, 2} {
				ctx, cancel := context.WithCancel(context.Background())
				calls := 0
				c := NewClient(retryTestDialer(func(string, time.Duration) (net.Conn, error) {
					calls++
					if phase == "cancel" {
						cancel()
					}
					if phase == "write" {
						return &retryWriteFailure{}, nil
					}
					return nil, io.ErrClosedPipe
				}), time.Second)
				_, nr, err := c.retry(ctx, &proto.Command{Type: proto.Command_COMMAND_TYPE_EXECUTE}, "unused", time.Second, retries)
				cancel()
				wantRetries := retries
				if phase == "cancel" {
					wantRetries = 0
				}
				if err == nil || calls != 1+wantRetries || nr != wantRetries {
					t.Fatalf("phase=%s budget=%d calls=%d retries=%d error=%v", phase, retries, calls, nr, err)
				}
			}
		})
	}
}
