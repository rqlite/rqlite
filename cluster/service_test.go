package cluster

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auth"
	"github.com/rqlite/rqlite/v10/cluster/proto"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/testdata/x509"
	pb "google.golang.org/protobuf/proto"
)

func Test_NewServiceOpenClose(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	if ml.Addr().String() != s.Addr() {
		t.Fatalf("service returned incorrect address")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetAPIAddr(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	if exp, got := "foo", s.GetAPIAddr(); exp != got {
		t.Fatalf("got incorrect API address, exp %s, got %s", exp, got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeMeta(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")
	s.SetVersion("1.0.0")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	c.SetLocalVersion("2.0.0")
	if v := c.GetLocalVersion(); v != "2.0.0" {
		t.Fatalf("failed to get correct local version, exp %s, got %s", "2.0.0", v)
	}
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "http://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "http://foo", meta.Url)
	}
	if meta.Version != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "2.0.0", meta.Version)
	}

	s.EnableHTTPS(true)

	// Test fetch via network.
	meta, err = c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", meta.Url)
	}
	if meta.Version != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "1.0.0", meta.Version)
	}

	// Test fetch via local call.
	addr := s.GetNodeAPIURL()
	if addr != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", addr)
	}
	ver := s.GetVersion()
	if ver != "1.0.0" {
		t.Fatalf("failed to get correct node version, exp %s, got %s", "1.0.0", ver)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceSetGetNodeMetaLocal(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Check stats to confirm no local request yet.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "0" {
		t.Fatalf("failed to confirm request served locally")
	}

	// Test by enabling local answering
	c := NewClient(ml, 30*time.Second)
	if err := c.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address locally: %s", err)
	}
	if meta.Url != "http://foo" {
		t.Fatalf("failed to get correct node API address locally, exp %s, got %s", "http://foo", meta.Url)
	}

	// Check stats to confirm local response.
	if stats.Get(numGetNodeAPIRequestLocal).String() != "1" {
		t.Fatalf("failed to confirm request served locally")
	}
}

func Test_NewServiceSetGetNodeMetaTLS(t *testing.T) {
	ml := mustNewMockTLSTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	s.SetAPIAddr("foo")

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	meta, err := c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	exp := "http://foo"
	if meta.Url != exp {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", exp, meta.Url)
	}

	s.EnableHTTPS(true)
	meta, err = c.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if meta.Url != "https://foo" {
		t.Fatalf("failed to get correct node API address, exp %s, got %s", "https://foo", meta.Url)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceGetCommitIndex(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	s.EnableHTTPS(true)

	// Test fetch via network.
	mgr.commitIndex = 1234
	c := NewClient(ml, 30*time.Second)
	idx, err := c.GetCommitIndex(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node API address: %s", err)
	}
	if idx != 1234 {
		t.Fatalf("failed to get correct node commit index, exp %d, got %d", 1234, idx)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_NewServiceTestExecuteQueryAuthNoCredentials(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	// Test that for a cluster with no credential store configured
	// all users are authed for both operations
	var c CredentialStore = nil
	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, _, err := cl.Execute(context.Background(), er, s.Addr(), nil, 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal(err)
	}
	qr := &command.QueryRequest{}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), nil, 5*time.Second, noRetries)
	if err != nil {
		t.Fatal(err)
	}
}

// Test_NewServiceTestExecuteQueryAuth tests that for a cluster with a credential
// store, configured users with execute permissions can execute and users with
// query permissions can query, and can't if they don't have those permissions.
func Test_NewServiceTestExecuteQueryAuth(t *testing.T) {
	ml := mustNewMockTransport()
	db := mustNewMockDatabase()
	clstr := mustNewMockManager()

	f := func(username string, password string, perm string) bool {
		if username == "alice" && password == "secret1" && perm == "execute" {
			return true
		} else if username == "bob" && password == "secret1" && perm == "query" {
			return true
		}
		return false
	}
	c := &mockCredentialStore{aaFunc: f}

	s := New(ml, db, clstr, c)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	cl := NewClient(ml, 30*time.Second)
	if err := cl.SetLocal(s.Addr(), s); err != nil {
		t.Fatalf("failed to set cluster client local parameters: %s", err)
	}
	er := &command.ExecuteRequest{}
	_, _, err := cl.Execute(context.Background(), er, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatal("alice improperly unauthorized to execute")
	}
	_, _, err = cl.Execute(context.Background(), er, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second, defaultMaxRetries)
	if err == nil {
		t.Fatal("bob improperly authorized to execute")
	}
	qr := &command.QueryRequest{}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), makeCredentials("bob", "secret1"), 5*time.Second, noRetries)
	if err != nil && err.Error() != "unauthorized" {
		fmt.Println(err)
		t.Fatal("bob improperly unauthorized to query")
	}
	_, _, err = cl.Query(context.Background(), qr, s.Addr(), makeCredentials("alice", "secret1"), 5*time.Second, noRetries)
	if err != nil && err.Error() != "unauthorized" {
		t.Fatal("alice improperly authorized to query")
	}
}

func Test_NewServiceNotify(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	var wg sync.WaitGroup

	wg.Add(1)
	mm.notifyFn = func(n *command.NotifyRequest) error {
		defer wg.Done()
		if n.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", n.Id)
		}
		if n.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", n.Address)
		}
		return nil
	}

	credStr := mustNewMockCredentialStore()
	s := New(ml, mustNewMockDatabase(), mm, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a notify request.
	nr := &command.NotifyRequest{
		Id:      "foo",
		Address: "localhost",
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Notify(context.Background(), nr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to notify node: %s", err)
	}

	// Ensure that the notify function was called.
	wg.Wait()

	// Test when auth is enabled
	credStr.HasPermOK = false
	err = c.Notify(context.Background(), nr, s.Addr(), nil, 5*time.Second)
	if err == nil {
		t.Fatal("should have failed to notify node due to lack of auth")
	}
	if err.Error() != "unauthorized" {
		t.Fatalf("failed to get correct error, exp %s, got %s", "unauthorized", err.Error())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

// errResponse is any cluster response message that carries an error string.
type errResponse interface {
	pb.Message
	GetError() string
}

// unauthorizedCommand describes one command that must be rejected when the
// credential store does not grant the permission it needs.
type unauthorizedCommand struct {
	name  string
	cmd   *proto.Command
	perms []string    // Permissions the store must be asked for, in order.
	resp  errResponse // Response type the client decodes for this command.
}

// unauthorizedCommands lists every command type that requires a permission,
// with the permissions the service must ask the credential store for. Join
// appears three times because its permission depends on the request.
func unauthorizedCommands() []unauthorizedCommand {
	return []unauthorizedCommand{
		{
			name: "EXECUTE",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_EXECUTE,
				Request: &proto.Command_ExecuteRequest{ExecuteRequest: executeRequestFromString("some SQL")},
			},
			perms: []string{auth.PermExecute},
			resp:  &proto.CommandExecuteResponse{},
		},
		{
			name: "QUERY",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_QUERY,
				Request: &proto.Command_QueryRequest{QueryRequest: queryRequestFromString("some SQL")},
			},
			perms: []string{auth.PermQuery},
			resp:  &proto.CommandQueryResponse{},
		},
		{
			name: "REQUEST",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_REQUEST,
				Request: &proto.Command_ExecuteQueryRequest{ExecuteQueryRequest: executeQueryRequestFromString("some SQL")},
			},
			// The check stops at the first permission the store refuses.
			perms: []string{auth.PermQuery},
			resp:  &proto.CommandRequestResponse{},
		},
		{
			name: "BACKUP",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_BACKUP,
				Request: &proto.Command_BackupRequest{BackupRequest: backupRequestBinary(true)},
			},
			perms: []string{auth.PermBackup},
			resp:  &proto.CommandBackupResponse{},
		},
		{
			name: "BACKUP_STREAM",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_BACKUP_STREAM,
				Request: &proto.Command_BackupRequest{BackupRequest: backupRequestBinary(true)},
			},
			perms: []string{auth.PermBackup},
			resp:  &proto.CommandBackupResponse{},
		},
		{
			name: "LOAD",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_LOAD,
				Request: &proto.Command_LoadRequest{LoadRequest: &command.LoadRequest{Data: []byte("sqlite")}},
			},
			perms: []string{auth.PermLoad},
			resp:  &proto.CommandLoadResponse{},
		},
		{
			name: "REMOVE_NODE",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_REMOVE_NODE,
				Request: &proto.Command_RemoveNodeRequest{RemoveNodeRequest: &command.RemoveNodeRequest{Id: "node1"}},
			},
			perms: []string{auth.PermRemove},
			resp:  &proto.CommandRemoveNodeResponse{},
		},
		{
			name: "DEMOTE_NODE",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_DEMOTE_NODE,
				Request: &proto.Command_DemoteNodeRequest{DemoteNodeRequest: &command.DemoteNodeRequest{Id: "node1"}},
			},
			perms: []string{auth.PermDemote},
			resp:  &proto.CommandDemoteNodeResponse{},
		},
		{
			name: "PROMOTE_NODE",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_PROMOTE_NODE,
				Request: &proto.Command_PromoteNodeRequest{PromoteNodeRequest: &command.PromoteNodeRequest{Id: "node1"}},
			},
			perms: []string{auth.PermPromote},
			resp:  &proto.CommandPromoteNodeResponse{},
		},
		{
			name: "NOTIFY",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_NOTIFY,
				Request: &proto.Command_NotifyRequest{NotifyRequest: &command.NotifyRequest{Id: "node1", Address: "localhost"}},
			},
			perms: []string{auth.PermJoin},
			resp:  &proto.CommandNotifyResponse{},
		},
		{
			name: "JOIN_voter",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_JOIN,
				Request: &proto.Command_JoinRequest{JoinRequest: &command.JoinRequest{Id: "node1", Address: "localhost", Voter: true}},
			},
			perms: []string{auth.PermJoin},
			resp:  &proto.CommandJoinResponse{},
		},
		{
			name: "JOIN_non_voter",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_JOIN,
				Request: &proto.Command_JoinRequest{JoinRequest: &command.JoinRequest{Id: "node1", Address: "localhost", Voter: false}},
			},
			perms: []string{auth.PermJoinReadOnly, auth.PermJoinReadReplica},
			resp:  &proto.CommandJoinResponse{},
		},
		{
			name: "JOIN_nil_request",
			cmd: &proto.Command{
				Type: proto.Command_COMMAND_TYPE_JOIN,
			},
			perms: []string{auth.PermJoin},
			resp:  &proto.CommandJoinResponse{},
		},
		{
			name: "STEPDOWN",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_STEPDOWN,
				Request: &proto.Command_StepdownRequest{StepdownRequest: &command.StepdownRequest{Wait: true}},
			},
			perms: []string{auth.PermLeaderOps},
			resp:  &proto.CommandStepdownResponse{},
		},
		{
			name: "HIGHWATER_MARK_UPDATE",
			cmd: &proto.Command{
				Type:    proto.Command_COMMAND_TYPE_HIGHWATER_MARK_UPDATE,
				Request: &proto.Command_HighwaterMarkUpdateRequest{HighwaterMarkUpdateRequest: &proto.HighwaterMarkUpdateRequest{HighwaterMark: 123}},
			},
			perms: []string{auth.PermCDCHWMUpdate},
			resp:  &proto.HighwaterMarkUpdateResponse{},
		},
	}
}

// openCommands lists the command types which need no permission.
var openCommands = []proto.Command_Type{
	proto.Command_COMMAND_TYPE_GET_NODE_META,
	proto.Command_COMMAND_TYPE_LOAD_CHUNK,
}

// mustReadClosed checks that nothing further arrives on conn and that the
// remote end has closed it. Since the service closes a connection only by
// returning from its handler, seeing EOF also proves the handler finished.
func mustReadClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("failed to set read deadline: %s", err)
	}
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	if n != 0 {
		t.Fatalf("node sent %d byte(s) after rejecting the command", n)
	}
	if err != io.EOF {
		t.Fatalf("expected EOF after rejected command, got %v", err)
	}
}

// Test_ServiceUnauthorizedClosesConnection sends every command type that
// requires a permission with credentials the store rejects, and checks that
// the node answers "unauthorized", asks the store for the right permission,
// touches neither the database nor the cluster manager, and then closes the
// connection without sending another byte. Each command is also sent with
// its request payload missing, which must not bypass the check.
func Test_ServiceUnauthorizedClosesConnection(t *testing.T) {
	for _, tc := range unauthorizedCommands() {
		for _, variant := range []struct {
			name string
			cmd  *proto.Command
		}{
			{"request", tc.cmd},
			{"no_request", &proto.Command{Type: tc.cmd.Type}},
		} {
			t.Run(tc.name+"/"+variant.name, func(t *testing.T) {
				// Every path into the database or cluster manager bumps this
				// counter, so any work done on a rejected command is caught.
				var touched atomic.Int64
				touch := func() { touched.Add(1) }
				db := &mockDatabase{
					executeFn: func(*command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
						touch()
						return nil, 0, nil
					},
					queryFn: func(*command.QueryRequest) ([]*command.QueryRows, uint64, error) {
						touch()
						return nil, 0, nil
					},
					requestFn: func(*command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
						touch()
						return nil, 0, 0, nil
					},
					backupFn: func(_ *command.BackupRequest, dst io.Writer) (int, error) {
						touch()
						return dst.Write(mustGZIPCompress([]byte("this is SQLite data")))
					},
					loadFn: func(*command.LoadRequest) error {
						touch()
						return nil
					},
				}
				mgr := &MockManager{
					removeNodeFn:  func(*command.RemoveNodeRequest) error { touch(); return nil },
					demoteNodeFn:  func(*command.DemoteNodeRequest) error { touch(); return nil },
					promoteNodeFn: func(*command.PromoteNodeRequest) error { touch(); return nil },
					notifyFn:      func(*command.NotifyRequest) error { touch(); return nil },
					joinFn:        func(*command.JoinRequest) error { touch(); return nil },
					stepdownFn:    func(bool, string) error { touch(); return nil },
				}

				var mu sync.Mutex
				var asked []string
				credStr := &mockCredentialStore{aaFunc: func(username, password, perm string) bool {
					mu.Lock()
					defer mu.Unlock()
					asked = append(asked, perm)
					return false
				}}

				ml := mustNewMockTransport()
				s := New(ml, db, mgr, credStr)
				if err := s.Open(); err != nil {
					t.Fatalf("failed to open cluster service: %s", err)
				}
				defer s.Close()
				hwmCh := make(chan uint64, 1)
				s.RegisterHWMUpdate(hwmCh)

				conn, err := ml.Dial(s.Addr(), 5*time.Second)
				if err != nil {
					t.Fatalf("failed to dial service: %s", err)
				}
				defer conn.Close()

				cmd := pb.Clone(variant.cmd).(*proto.Command)
				cmd.Credentials = makeCredentials("user", "wrong")
				if err := writeCommand(conn, cmd, 5*time.Second); err != nil {
					t.Fatalf("failed to write command: %s", err)
				}
				p, err := readResponse(conn, 5*time.Second)
				if err != nil {
					t.Fatalf("failed to read response: %s", err)
				}
				if cmd.Type == proto.Command_COMMAND_TYPE_BACKUP {
					// Non-streaming backup responses are compressed on the wire.
					if p, err = mustGZIPUncompress(p); err != nil {
						t.Fatalf("failed to decompress response: %s", err)
					}
				}
				resp := pb.Clone(tc.resp).(errResponse)
				if err := pb.Unmarshal(p, resp); err != nil {
					t.Fatalf("failed to unmarshal response: %s", err)
				}
				if resp.GetError() != "unauthorized" {
					t.Fatalf("failed to get correct error, exp %s, got %q", "unauthorized", resp.GetError())
				}

				// The node must now close the connection. Once it has, the
				// handler has returned, so any work it did is visible below.
				mustReadClosed(t, conn)

				if n := touched.Load(); n != 0 {
					t.Fatalf("database or cluster manager was called %d time(s) on a rejected command", n)
				}
				if len(hwmCh) != 0 {
					t.Fatalf("highwater mark was delivered on a rejected command")
				}
				if variant.name == "request" {
					mu.Lock()
					defer mu.Unlock()
					if !slices.Equal(asked, tc.perms) {
						t.Fatalf("credential store asked for %v, exp %v", asked, tc.perms)
					}
				}
			})
		}
	}
}

// Test_ServiceOpenCommandsNeedNoPermission checks that the command types
// which need no permission are still served, on a connection that stays
// open, even when the credential store rejects everything.
func Test_ServiceOpenCommandsNeedNoPermission(t *testing.T) {
	var asked atomic.Int64
	credStr := &mockCredentialStore{aaFunc: func(string, string, string) bool {
		asked.Add(1)
		return false
	}}
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), credStr)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err)
	}
	defer s.Close()

	conn, err := ml.Dial(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to dial service: %s", err)
	}
	defer conn.Close()

	// Send each open command twice on the same connection, to prove the
	// connection is left open after each one.
	for i := 0; i < 2; i++ {
		cmd := &proto.Command{Type: proto.Command_COMMAND_TYPE_GET_NODE_META}
		if err := writeCommand(conn, cmd, 5*time.Second); err != nil {
			t.Fatalf("failed to write node meta command: %s", err)
		}
		p, err := readResponse(conn, 5*time.Second)
		if err != nil {
			t.Fatalf("failed to read node meta response: %s", err)
		}
		if err := pb.Unmarshal(p, &proto.NodeMeta{}); err != nil {
			t.Fatalf("failed to unmarshal node meta response: %s", err)
		}

		cmd = &proto.Command{
			Type:    proto.Command_COMMAND_TYPE_LOAD_CHUNK,
			Request: &proto.Command_LoadChunkRequest{LoadChunkRequest: &command.LoadChunkRequest{}},
		}
		if err := writeCommand(conn, cmd, 5*time.Second); err != nil {
			t.Fatalf("failed to write load chunk command: %s", err)
		}
		p, err = readResponse(conn, 5*time.Second)
		if err != nil {
			t.Fatalf("failed to read load chunk response: %s", err)
		}
		resp := &proto.CommandLoadChunkResponse{}
		if err := pb.Unmarshal(p, resp); err != nil {
			t.Fatalf("failed to unmarshal load chunk response: %s", err)
		}
		if resp.GetError() != "unsupported" {
			t.Fatalf("failed to get correct error, exp %s, got %q", "unsupported", resp.GetError())
		}
	}
	if n := asked.Load(); n != 0 {
		t.Fatalf("credential store was asked %d time(s) for commands that need no permission", n)
	}
}

// Test_ServiceUnknownCommandClosesConnection checks that a command type the
// node does not recognise gets no response and closes the connection, even
// when no credential store is configured.
func Test_ServiceUnknownCommandClosesConnection(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), nil)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service: %s", err)
	}
	defer s.Close()

	conn, err := ml.Dial(s.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("failed to dial service: %s", err)
	}
	defer conn.Close()

	cmd := &proto.Command{Type: proto.Command_COMMAND_TYPE_UNKNOWN}
	if err := writeCommand(conn, cmd, 5*time.Second); err != nil {
		t.Fatalf("failed to write command: %s", err)
	}
	mustReadClosed(t, conn)
}

// Test_ServiceEveryCommandTypeIsCovered fails when a command type exists
// that neither the unauthorized tests nor the open-command list knows
// about. Adding a command type means deciding, here, whether it needs a
// permission, and adding it to the matching table.
func Test_ServiceEveryCommandTypeIsCovered(t *testing.T) {
	covered := map[proto.Command_Type]bool{
		proto.Command_COMMAND_TYPE_UNKNOWN: true, // Test_ServiceUnknownCommandClosesConnection
	}
	for _, tc := range unauthorizedCommands() {
		covered[tc.cmd.Type] = true
	}
	for _, typ := range openCommands {
		covered[typ] = true
	}
	for v, name := range proto.Command_Type_name {
		if !covered[proto.Command_Type(v)] {
			t.Errorf("command type %s has no authorization test; add it to unauthorizedCommands or openCommands", name)
		}
	}
}

func Test_NewServiceJoin(t *testing.T) {
	ml := mustNewMockTransport()
	mm := mustNewMockManager()
	var wg sync.WaitGroup

	wg.Add(1)
	mm.joinFn = func(j *command.JoinRequest) error {
		defer wg.Done()
		if j.Id != "foo" {
			t.Fatalf("failed to get correct node ID, exp %s, got %s", "foo", j.Id)
		}
		if j.Address != "localhost" {
			t.Fatalf("failed to get correct node address, exp %s, got %s", "localhost", j.Address)
		}
		if !j.Voter {
			t.Fatalf("failed to get correct voter setting, exp %t, got %t", true, j.Voter)
		}
		return nil
	}

	credStr := mustNewMockCredentialStore()
	s := New(ml, mustNewMockDatabase(), mm, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}

	// Create a Join request.
	jr := &command.JoinRequest{
		Id:      "foo",
		Address: "localhost",
		Voter:   true,
	}

	// Test by connecting to itself.
	c := NewClient(ml, 30*time.Second)
	err := c.Join(context.Background(), jr, s.Addr(), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to join node: %s", err)
	}

	// Ensure the join function was called.
	wg.Wait()

	// Test when auth is enabled
	credStr.HasPermOK = false
	err = c.Join(context.Background(), jr, s.Addr(), nil, 5*time.Second)
	if err == nil {
		t.Fatal("should have failed to join node due to lack of auth")
	}
	if err.Error() != "unauthorized" {
		t.Fatalf("failed to get correct error, exp %s, got %s", "unauthorized", err.Error())
	}

	if err := s.Close(); err != nil {
		t.Fatalf("failed to close cluster service")
	}
}

func Test_ServiceHandleHighwaterMarkUpdate(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	responses, err := c.BroadcastHWM(context.Background(), 987654, nil, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}
}

func Test_ServiceRegisterHWMUpdate(t *testing.T) {
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	s := New(ml, mustNewMockDatabase(), mgr, mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a channel to receive highwater mark updates
	hwmCh := make(chan uint64, 1)
	s.RegisterHWMUpdate(hwmCh)

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	testHWM := uint64(123456)
	responses, err := c.BroadcastHWM(context.Background(), testHWM, nil, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}

	// Check that we received the update on the channel
	select {
	case hwmUpdate := <-hwmCh:
		if hwmUpdate != testHWM {
			t.Fatalf("expected highwater_mark to be %d, got: %d", testHWM, hwmUpdate)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for highwater mark update on channel")
	}
}

// Test_ServiceRegisterHWMUpdate_WithAuth checks that the CDC service checks credentials.
func Test_ServiceRegisterHWMUpdate_WithAuth(t *testing.T) {
	creds := &proto.Credentials{Username: "bob", Password: "passwd"}
	ml := mustNewMockTransport()
	mgr := mustNewMockManager()
	credStr := mustNewMockCredentialStore()
	credStr.aaFunc = func(username, password, perm string) bool {
		return username == creds.Username && password == creds.Password
	}
	s := New(ml, mustNewMockDatabase(), mgr, credStr)
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Create a client and send highwater mark update
	c := NewClient(ml, 30*time.Second)
	c.SetLocal("test-node", nil)

	// Use the client to send a highwater mark update
	testHWM := uint64(123456)
	responses, err := c.BroadcastHWM(context.Background(), testHWM, creds, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}

	// Check that we got a response for the service address
	resp, ok := responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}

	// Check response has no error
	if resp.Error != "" {
		t.Fatalf("expected no error, got: %s", resp.Error)
	}

	// Change required creds and ensure auth kicks in to deny.
	credStr.aaFunc = func(username, password, perm string) bool {
		return username == creds.Username && password == "foo"
	}
	responses, err = c.BroadcastHWM(context.Background(), testHWM, creds, 0, 5*time.Second, s.Addr())
	if err != nil {
		t.Fatalf("failed to broadcast highwater mark update: %s", err)
	}
	resp, ok = responses[s.Addr()]
	if !ok {
		t.Fatalf("expected response for address %s", s.Addr())
	}
	if resp.Error != "unauthorized" {
		t.Fatal("expected an error")
	}
}

func Test_ServiceClosesIdleConnection(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}
	s.connTimeout = 500 * time.Millisecond

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	// Connect but send nothing. The service should close the connection
	// after ConnReadTimeout.
	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("failed to connect to service: %s", err)
	}
	defer conn.Close()

	// The server should close the connection after the read timeout.
	one := make([]byte, 1)
	_, err = conn.Read(one)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got: %v", err)
	}
}

func Test_ServiceSetConnectionLimitOpen(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	if err := s.SetConnectionLimit(1); !errors.Is(err, ErrServiceOpen) {
		t.Fatalf("expected ErrServiceOpen, got: %v", err)
	}
}

func Test_ServiceConnLimiterRejectAndRelease(t *testing.T) {
	ml := mustNewMockTransport()
	s := New(ml, mustNewMockDatabase(), mustNewMockManager(), mustNewMockCredentialStore())
	if err := s.SetConnectionLimit(1); err != nil {
		t.Fatalf("failed to set connection limit: %s", err)
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	md := &mockDialer{Dialer: ml}
	t.Cleanup(md.CloseAll)

	c1 := NewClient(md, 30*time.Second)
	_, err := c1.GetNodeMeta(context.Background(), s.Addr(), noRetries, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to get node metadata on first connection: %s", err)
	}

	// The first client's pooled connection remains open and holds the limiter slot.
	c2 := NewClient(md, 30*time.Second)
	_, err = c2.GetNodeMeta(context.Background(), s.Addr(), noRetries, 500*time.Millisecond)
	if err == nil {
		t.Fatalf("expected request on rejected connection to fail")
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		t.Fatalf("expected connection to be rejected without timing out, got: %v", err)
	}

	// Releasing the idle connection should free its limiter slot.
	if err := md.CloseConn(0); err != nil {
		t.Fatalf("failed to close first connection: %s", err)
	}

	// A new client request should be accepted once capacity is available again.
	c3 := NewClient(md, 30*time.Second)
	_, err = c3.GetNodeMeta(context.Background(), s.Addr(), noRetries, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to get node metadata after release: %s", err)
	}
}

type mockDialer struct {
	Dialer
	mu    sync.Mutex
	conns []net.Conn
}

func (d *mockDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := d.Dialer.Dial(address, timeout)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()

	return conn, nil
}

func (d *mockDialer) CloseConn(index int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if index < 0 || index >= len(d.conns) {
		return fmt.Errorf("connection %d not tracked", index)
	}
	return d.conns[index].Close()
}

func (d *mockDialer) CloseAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
}

type mockTransport struct {
	tn              net.Listener
	remoteEncrypted bool
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

func (ml *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if ml.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}

	return conn, err
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

func mustNewMockTLSTransport() *mockTransport {
	tn := mustNewMockTransport()
	return &mockTransport{
		tn:              tls.NewListener(tn, mustCreateTLSConfig()),
		remoteEncrypted: true,
	}
}

type mockDatabase struct {
	executeFn func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error)
	queryFn   func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error)
	requestFn func(rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error)
	backupFn  func(br *command.BackupRequest, dst io.Writer) (int, error)
	loadFn    func(lr *command.LoadRequest) error
}

func (m *mockDatabase) Execute(ctx context.Context, er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
	return m.executeFn(er)
}

func (m *mockDatabase) Query(ctx context.Context, qr *command.QueryRequest) ([]*command.QueryRows, command.ConsistencyLevel, uint64, error) {
	rows, idx, err := m.queryFn(qr)
	return rows, command.ConsistencyLevel_NONE, idx, err
}

func (m *mockDatabase) Request(ctx context.Context, rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
	if m.requestFn == nil {
		return []*command.ExecuteQueryResponse{}, 0, 0, nil
	}
	return m.requestFn(rr)
}

func (m *mockDatabase) Backup(ctx context.Context, br *command.BackupRequest, dst io.Writer) (int, error) {
	if m.backupFn == nil {
		return 0, nil
	}
	return m.backupFn(br, dst)
}

func (m *mockDatabase) Load(ctx context.Context, lr *command.LoadRequest) error {
	if m.loadFn == nil {
		return nil
	}
	return m.loadFn(lr)
}

func mustNewMockDatabase() *mockDatabase {
	e := func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		return []*command.ExecuteQueryResponse{}, 0, nil
	}
	q := func(er *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
		return []*command.QueryRows{}, 0, nil
	}
	return &mockDatabase{executeFn: e, queryFn: q}
}

type MockManager struct {
	removeNodeFn  func(rn *command.RemoveNodeRequest) error
	demoteNodeFn  func(dn *command.DemoteNodeRequest) error
	promoteNodeFn func(pn *command.PromoteNodeRequest) error
	notifyFn      func(n *command.NotifyRequest) error
	joinFn        func(j *command.JoinRequest) error
	leaderAddrFn  func() (string, error)
	stepdownFn    func(wait bool, id string) error
	commitIndex   uint64
}

func (m *MockManager) Remove(ctx context.Context, rn *command.RemoveNodeRequest) error {
	if m.removeNodeFn == nil {
		return nil
	}
	return m.removeNodeFn(rn)
}

func (m *MockManager) Demote(ctx context.Context, dn *command.DemoteNodeRequest) error {
	if m.demoteNodeFn == nil {
		return nil
	}
	return m.demoteNodeFn(dn)
}

func (m *MockManager) Promote(ctx context.Context, pn *command.PromoteNodeRequest) error {
	if m.promoteNodeFn == nil {
		return nil
	}
	return m.promoteNodeFn(pn)
}

func (m *MockManager) Notify(n *command.NotifyRequest) error {
	if m.notifyFn == nil {
		return nil
	}
	return m.notifyFn(n)
}

func (m *MockManager) Join(j *command.JoinRequest) error {
	if m.joinFn == nil {
		return nil
	}
	return m.joinFn(j)
}

func (m *MockManager) Stepdown(wait bool, id string) error {
	if m.stepdownFn == nil {
		return nil
	}
	return m.stepdownFn(wait, id)
}

func (m *MockManager) LeaderAddr() (string, error) {
	if m.leaderAddrFn == nil {
		return "", nil
	}
	return m.leaderAddrFn()
}

func (m *MockManager) CommitIndex() (uint64, error) {
	return m.commitIndex, nil
}

func mustNewMockManager() *MockManager {
	return &MockManager{}
}

func mustCreateTLSConfig() *tls.Config {
	var err error

	certFile := x509.CertExampleDotComFile("")
	defer fsutil.Remove(certFile)
	keyFile := x509.KeyExampleDotComFile("")
	defer fsutil.Remove(keyFile)

	config := &tls.Config{
		InsecureSkipVerify: true,
	}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic("failed to create TLS config")
	}

	return config
}

type mockCredentialStore struct {
	HasPermOK bool
	aaFunc    func(username, password, perm string) bool
}

func (m *mockCredentialStore) AA(username, password, perm string) bool {
	if m == nil {
		return true
	}

	if m.aaFunc != nil {
		return m.aaFunc(username, password, perm)
	}
	return m.HasPermOK
}

func mustNewMockCredentialStore() *mockCredentialStore {
	return &mockCredentialStore{HasPermOK: true}
}

// mustGZIPUncompress returns the decompressed form of gzip data.
func mustGZIPUncompress(b []byte) ([]byte, error) {
	gzr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer gzr.Close()
	return io.ReadAll(gzr)
}

func makeCredentials(username, password string) *proto.Credentials {
	return &proto.Credentials{
		Username: username,
		Password: password,
	}
}

// Test_NewServiceErrorV2RoundTrip ensures the structured SQLite error on
// execute, query, and request results survives being forwarded between nodes.
// The client does not set a local service, so every call crosses the network.
func Test_NewServiceErrorV2RoundTrip(t *testing.T) {
	ml := mustNewMockTransport()
	wantErr := &command.Error{Message: "constraint failed", Sqlite: &command.SQLiteErrorCodes{Code: 19, ExtendedCode: 1555, SystemErrno: 0}}
	db := &mockDatabase{
		executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
			return []*command.ExecuteQueryResponse{{
				Result: &command.ExecuteQueryResponse_E{
					E: &command.ExecuteResult{Error: "constraint failed", ErrorV2: wantErr},
				},
			}}, 1, nil
		},
		queryFn: func(qr *command.QueryRequest) ([]*command.QueryRows, uint64, error) {
			return []*command.QueryRows{{Error: "no such table", ErrorV2: wantErr}}, 1, nil
		},
		requestFn: func(rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, uint64, uint64, error) {
			return []*command.ExecuteQueryResponse{
				{Result: &command.ExecuteQueryResponse_E{
					E: &command.ExecuteResult{Error: "constraint failed", ErrorV2: wantErr},
				}},
				{Result: &command.ExecuteQueryResponse_Q{
					Q: &command.QueryRows{Error: "no such table", ErrorV2: wantErr},
				}},
			}, 1, 1, nil
		},
	}

	s := New(ml, db, mustNewMockManager(), mustNewMockCredentialStore())
	if s == nil {
		t.Fatalf("failed to create cluster service")
	}
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open cluster service")
	}
	defer s.Close()

	checkErr := func(t *testing.T, what string, got *command.Error) {
		t.Helper()
		if got == nil {
			t.Fatalf("%s: structured error lost in transit", what)
		}
		gs, ws := got.GetSqlite(), wantErr.GetSqlite()
		if got.GetMessage() != wantErr.GetMessage() || gs == nil || gs.GetCode() != ws.GetCode() ||
			gs.GetExtendedCode() != ws.GetExtendedCode() || gs.GetSystemErrno() != ws.GetSystemErrno() {
			t.Fatalf("%s: unexpected structured error: got %v, want %v", what, got, wantErr)
		}
	}

	cl := NewClient(ml, 30*time.Second)
	res, _, err := cl.Execute(context.Background(), &command.ExecuteRequest{}, s.Addr(), nil, 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatalf("failed to execute: %s", err)
	}
	if len(res) != 1 || res[0].GetE().GetError() != "constraint failed" {
		t.Fatalf("unexpected execute results: %v", res)
	}
	checkErr(t, "execute", res[0].GetE().GetErrorV2())

	rows, _, err := cl.Query(context.Background(), &command.QueryRequest{}, s.Addr(), nil, 5*time.Second, noRetries)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}
	if len(rows) != 1 || rows[0].GetError() != "no such table" {
		t.Fatalf("unexpected query results: %v", rows)
	}
	checkErr(t, "query", rows[0].GetErrorV2())

	res, _, _, err = cl.Request(context.Background(), &command.ExecuteQueryRequest{}, s.Addr(), nil, 5*time.Second, defaultMaxRetries)
	if err != nil {
		t.Fatalf("failed to request: %s", err)
	}
	if len(res) != 2 {
		t.Fatalf("unexpected request results: %v", res)
	}
	checkErr(t, "request execute", res[0].GetE().GetErrorV2())
	checkErr(t, "request query", res[1].GetQ().GetErrorV2())
}
