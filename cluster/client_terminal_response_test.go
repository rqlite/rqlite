package cluster

import (
	"net"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/cluster/proto"
	"github.com/rqlite/rqlite/v10/tcp/pool"
	pb "google.golang.org/protobuf/proto"
)

// TestClientTerminalResponse verifies only connection-closing responses discard sockets.
func TestClientTerminalResponse(t *testing.T) {
	types := []proto.Command_Type{proto.Command_COMMAND_TYPE_EXECUTE, proto.Command_COMMAND_TYPE_QUERY,
		proto.Command_COMMAND_TYPE_REQUEST, proto.Command_COMMAND_TYPE_LOAD, proto.Command_COMMAND_TYPE_LOAD_CHUNK,
		proto.Command_COMMAND_TYPE_BACKUP_STREAM, proto.Command_COMMAND_TYPE_JOIN, proto.Command_COMMAND_TYPE_NOTIFY,
		proto.Command_COMMAND_TYPE_REMOVE_NODE, proto.Command_COMMAND_TYPE_DEMOTE_NODE, proto.Command_COMMAND_TYPE_PROMOTE_NODE,
		proto.Command_COMMAND_TYPE_STEPDOWN, proto.Command_COMMAND_TYPE_HIGHWATER_MARK_UPDATE}
	for _, kind := range types {
		t.Run(kind.String(), func(t *testing.T) {
			for _, msg := range []string{unauthorizedError, unsupportedError, "not leader", "query includes unauthorized", "BackupRequest is nil"} {
				t.Run(msg, func(t *testing.T) {
					a, b := net.Pipe()
					defer b.Close()
					p, _ := pool.NewChannelPool(1, func() (net.Conn, error) { return a, nil })
					defer p.Close()
					conn, err := p.Get()
					if err != nil {
						t.Fatal(err)
					}
					done := make(chan struct{})
					go func() { defer close(done); data, _ := errorResponse(kind, msg); writeBytesWithLength(b, data) }()
					data, err := readCommandResponse(conn, &proto.Command{Type: kind}, time.Second)
					if err != nil {
						t.Fatal(err)
					}
					var resp proto.CommandExecuteResponse
					if err := pb.Unmarshal(data, &resp); err != nil || resp.Error != msg {
						t.Fatalf("error not preserved: %v %q", err, resp.Error)
					}
					conn.Close()
					<-done
					want := 1
					if msg == unauthorizedError || msg == unsupportedError || (kind == proto.Command_COMMAND_TYPE_BACKUP_STREAM && msg == "BackupRequest is nil") {
						want = 0
					}
					if p.Len() != want {
						t.Fatalf("idle=%d want=%d", p.Len(), want)
					}
				})
			}
		})
	}
}
