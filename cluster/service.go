package cluster

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/auth"
	"github.com/rqlite/rqlite/v8/cluster/proto"
	command "github.com/rqlite/rqlite/v8/command/proto"
	pb "google.golang.org/protobuf/proto"
)

// stats captures stats for the Cluster service.
var stats *expvar.Map

const (
	numGetNodeAPIRequest  = "num_get_node_api_req"
	numGetNodeAPIResponse = "num_get_node_api_resp"
	numExecuteRequest     = "num_execute_req"
	numQueryRequest       = "num_query_req"
	numRequestRequest     = "num_request_req"
	numBackupRequest      = "num_backup_req"
	numLoadRequest        = "num_load_req"
	numRemoveNodeRequest  = "num_remove_node_req"
	numNotifyRequest      = "num_notify_req"
	numJoinRequest        = "num_join_req"

	numClientRetries            = "num_client_retries"
	numGetNodeAPIRequestRetries = "num_get_node_api_req_retries"
	numClientLoadRetries        = "num_client_load_retries"
	numClientExecuteRetries     = "num_client_execute_retries"
	numClientQueryRetries       = "num_client_query_retries"
	numClientRequestRetries     = "num_client_request_retries"
	numClientReadTimeouts       = "num_client_read_timeouts"
	numClientWriteTimeouts      = "num_client_write_timeouts"

	// Client stats for this package.
	numGetNodeAPIRequestLocal = "num_get_node_api_req_local"
)

const (
	// MuxRaftHeader is the byte used to indicate internode Raft communications.
	MuxRaftHeader = 1

	// MuxClusterHeader is the byte used to request internode cluster state information.
	MuxClusterHeader = 2 // Cluster state communications
)

func init() {
	stats = expvar.NewMap("cluster")
	stats.Add(numGetNodeAPIRequest, 0)
	stats.Add(numGetNodeAPIResponse, 0)
	stats.Add(numExecuteRequest, 0)
	stats.Add(numQueryRequest, 0)
	stats.Add(numRequestRequest, 0)
	stats.Add(numBackupRequest, 0)
	stats.Add(numLoadRequest, 0)
	stats.Add(numRemoveNodeRequest, 0)
	stats.Add(numGetNodeAPIRequestLocal, 0)
	stats.Add(numNotifyRequest, 0)
	stats.Add(numJoinRequest, 0)
	stats.Add(numClientRetries, 0)
	stats.Add(numGetNodeAPIRequestRetries, 0)
	stats.Add(numClientLoadRetries, 0)
	stats.Add(numClientExecuteRetries, 0)
	stats.Add(numClientQueryRetries, 0)
	stats.Add(numClientRequestRetries, 0)
	stats.Add(numClientReadTimeouts, 0)
	stats.Add(numClientWriteTimeouts, 0)
}

// Dialer is the interface dialers must implement.
type Dialer interface {
	// Dial is used to create a connection to a service listening
	// on an address.
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Database is the interface any queryable system must implement
type Database interface {
	// Execute executes a slice of SQL statements.
	Execute(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, error)

	// Query executes a slice of queries, each of which returns rows.
	Query(qr *command.QueryRequest) ([]*command.QueryRows, error)

	// Request processes a request that can both executes and queries.
	Request(rr *command.ExecuteQueryRequest) ([]*command.ExecuteQueryResponse, error)

	// Backup writes a backup of the database to the writer.
	Backup(br *command.BackupRequest, dst io.Writer) error

	// Loads an entire SQLite file into the database
	Load(lr *command.LoadRequest) error
}

// Manager is the interface node-management systems must implement
type Manager interface {
	// LeaderAddr returns the Raft address of the leader of the cluster.
	LeaderAddr() (string, error)

	// CommitIndex returns the Raft commit index of the cluster.
	// If verifyLeader is true, it verifies that this node is still the leader
	CommitIndex(enforceLeader bool, verifyLeader bool) (uint64, error)

	// Remove removes the node, given by id, from the cluster
	Remove(rn *command.RemoveNodeRequest) error

	// Notify notifies this node that a remote node is ready
	// for bootstrapping.
	Notify(n *command.NotifyRequest) error

	// Join joins a remote node to the cluster.
	Join(n *command.JoinRequest) error
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// AA authenticates and checks authorization for the given perm.
	AA(username, password, perm string) bool
}

// Service provides information about the node and cluster.
type Service struct {
	ln   net.Listener // Incoming connections to the service
	addr net.Addr     // Address on which this service is listening

	db  Database // The queryable system.
	mgr Manager  // The cluster management system.

	credentialStore CredentialStore

	mu      sync.RWMutex
	https   bool   // Serving HTTPS?
	apiAddr string // host:port this node serves the HTTP API.

	logger *log.Logger
}

// New returns a new instance of the cluster service
func New(ln net.Listener, db Database, m Manager, credentialStore CredentialStore) *Service {
	return &Service{
		ln:              ln,
		addr:            ln.Addr(),
		db:              db,
		mgr:             m,
		logger:          log.New(os.Stderr, "[cluster] ", log.LstdFlags),
		credentialStore: credentialStore,
	}
}

// Open opens the Service.
func (s *Service) Open() error {
	go s.serve()
	s.logger.Println("service listening on", s.addr)
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.ln.Close()
	return nil
}

// Addr returns the address the service is listening on.
func (s *Service) Addr() string {
	return s.addr.String()
}

// EnableHTTPS tells the cluster service the API serves HTTPS.
func (s *Service) EnableHTTPS(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.https = b
}

// SetAPIAddr sets the API address the cluster service returns.
func (s *Service) SetAPIAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apiAddr = addr
}

// GetAPIAddr returns the previously-set API address
func (s *Service) GetAPIAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.apiAddr
}

// GetNodeAPIURL returns fully-specified HTTP(S) API URL for the
// node running this service.
func (s *Service) GetNodeAPIURL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scheme := "http"
	if s.https {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, s.apiAddr)
}

// Stats returns status of the Service.
func (s *Service) Stats() (map[string]interface{}, error) {
	st := map[string]interface{}{
		"addr":     s.addr.String(),
		"https":    strconv.FormatBool(s.https),
		"api_addr": s.apiAddr,
	}

	return st, nil
}

func (s *Service) serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) checkCommandPerm(c *proto.Command, perm string) bool {
	if s.credentialStore == nil {
		return true
	}

	username := ""
	password := ""
	if c.Credentials != nil {
		username = c.Credentials.GetUsername()
		password = c.Credentials.GetPassword()
	}
	return s.credentialStore.AA(username, password, perm)
}

func (s *Service) checkCommandPermAll(c *proto.Command, perms ...string) bool {
	if s.credentialStore == nil {
		return true
	}

	username := ""
	password := ""
	if c.Credentials != nil {
		username = c.Credentials.GetUsername()
		password = c.Credentials.GetPassword()
	}
	for _, perm := range perms {
		if !s.credentialStore.AA(username, password, perm) {
			return false
		}
	}
	return true
}

func (s *Service) handleConn(conn net.Conn) {
	defer conn.Close()

	b := make([]byte, protoBufferLengthSize)
	for {
		_, err := io.ReadFull(conn, b)
		if err != nil {
			return
		}
		sz := binary.LittleEndian.Uint64(b[0:])

		p := make([]byte, sz)
		_, err = io.ReadFull(conn, p)
		if err != nil {
			return
		}

		c := &proto.Command{}
		err = pb.Unmarshal(p, c)
		if err != nil {
			conn.Close()
		}

		switch c.Type {
		case proto.Command_COMMAND_TYPE_GET_NODE_API_URL:
			stats.Add(numGetNodeAPIRequest, 1)
			resp := &proto.NodeMeta{}

			enforceLeader, verifyLeader := false, false
			ir := c.GetGetNodeApiUrlRequest()
			if ir != nil {
				enforceLeader = true
				verifyLeader = ir.VerifyLeader
			}

			ci, err := s.mgr.CommitIndex(enforceLeader, verifyLeader)
			if err != nil {
				resp.Error = err.Error()
			}

			resp.Url = s.GetNodeAPIURL()
			resp.CommitIndex = ci

			p, err = pb.Marshal(resp)
			if err != nil {
				conn.Close()
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}
			stats.Add(numGetNodeAPIResponse, 1)

		case proto.Command_COMMAND_TYPE_EXECUTE:
			stats.Add(numExecuteRequest, 1)
			resp := &proto.CommandExecuteResponse{}

			er := c.GetExecuteRequest()
			if er == nil {
				resp.Error = "ExecuteRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermExecute) {
				resp.Error = "unauthorized"
			} else {
				res, err := s.db.Execute(er)
				if err != nil {
					resp.Error = err.Error()
				} else {
					resp.Response = make([]*command.ExecuteQueryResponse, len(res))
					copy(resp.Response, res)
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_QUERY:
			stats.Add(numQueryRequest, 1)
			resp := &proto.CommandQueryResponse{}

			qr := c.GetQueryRequest()
			if qr == nil {
				resp.Error = "QueryRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermQuery) {
				resp.Error = "unauthorized"
			} else {
				res, err := s.db.Query(qr)
				if err != nil {
					resp.Error = err.Error()
				} else {
					resp.Rows = make([]*command.QueryRows, len(res))
					copy(resp.Rows, res)
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_REQUEST:
			stats.Add(numRequestRequest, 1)
			resp := &proto.CommandRequestResponse{}

			rr := c.GetExecuteQueryRequest()
			if rr == nil {
				resp.Error = "RequestRequest is nil"
			} else if !s.checkCommandPermAll(c, auth.PermQuery, auth.PermExecute) {
				resp.Error = "unauthorized"
			} else {
				res, err := s.db.Request(rr)
				if err != nil {
					resp.Error = err.Error()
				} else {
					resp.Response = make([]*command.ExecuteQueryResponse, len(res))
					copy(resp.Response, res)
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_BACKUP:
			stats.Add(numBackupRequest, 1)
			resp := &proto.CommandBackupResponse{}

			br := c.GetBackupRequest()
			if br == nil {
				resp.Error = "BackupRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermBackup) {
				resp.Error = "unauthorized"
			} else {
				buf := new(bytes.Buffer)
				if err := s.db.Backup(br, buf); err != nil {
					resp.Error = err.Error()
				} else {
					resp.Data = buf.Bytes()
				}
			}
			p, err = pb.Marshal(resp)
			if err != nil {
				conn.Close()
				return
			}

			// Compress the backup for less space on the wire between nodes.
			p, err = gzCompress(p)
			if err != nil {
				conn.Close()
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_BACKUP_STREAM:
			stats.Add(numBackupRequest, 1)
			resp := &proto.CommandBackupResponse{}

			br := c.GetBackupRequest()
			if br == nil {
				resp.Error = "BackupRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermBackup) {
				resp.Error = "unauthorized"
			}
			p, err = pb.Marshal(resp)
			if err != nil {
				conn.Close()
				return
			}
			if err := writeBytesWithLength(conn, p); err != nil {
				return
			}

			// Now, start streaming the backup. Enable compressed mode
			// regardless of whether the client requested it, so the client
			// can easily detect the end of the stream, as well as saving
			// space on the wire.
			br.Compress = true
			if err := s.db.Backup(br, conn); err != nil {
				s.logger.Printf("failed to stream backup: %s", err.Error())
				return
			}

		case proto.Command_COMMAND_TYPE_LOAD:
			stats.Add(numLoadRequest, 1)
			resp := &proto.CommandLoadResponse{}

			lr := c.GetLoadRequest()
			if lr == nil {
				resp.Error = "LoadRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermLoad) {
				resp.Error = "unauthorized"
			} else {
				if err := s.db.Load(lr); err != nil {
					resp.Error = fmt.Sprintf("remote node failed to load: %s", err.Error())
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				conn.Close()
				return
			}

		case proto.Command_COMMAND_TYPE_LOAD_CHUNK:
			resp := &proto.CommandLoadChunkResponse{
				Error: "unsupported",
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_REMOVE_NODE:
			stats.Add(numRemoveNodeRequest, 1)
			resp := &proto.CommandRemoveNodeResponse{}

			rn := c.GetRemoveNodeRequest()
			if rn == nil {
				resp.Error = "LoadRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermRemove) {
				resp.Error = "unauthorized"
			} else {
				if err := s.mgr.Remove(rn); err != nil {
					resp.Error = err.Error()
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_NOTIFY:
			stats.Add(numNotifyRequest, 1)
			resp := &proto.CommandNotifyResponse{}

			nr := c.GetNotifyRequest()
			if nr == nil {
				resp.Error = "NotifyRequest is nil"
			} else if !s.checkCommandPerm(c, auth.PermJoin) {
				resp.Error = "unauthorized"
			} else {
				if err := s.mgr.Notify(nr); err != nil {
					resp.Error = err.Error()
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}

		case proto.Command_COMMAND_TYPE_JOIN:
			stats.Add(numJoinRequest, 1)
			resp := &proto.CommandJoinResponse{}

			jr := c.GetJoinRequest()
			if jr == nil {
				resp.Error = "JoinRequest is nil"
			} else {
				if (jr.Voter && s.checkCommandPerm(c, auth.PermJoin)) ||
					(!jr.Voter && s.checkCommandPerm(c, auth.PermJoinReadOnly)) {
					if err := s.mgr.Join(jr); err != nil {
						resp.Error = err.Error()
						if err.Error() == "not leader" {
							laddr, err := s.mgr.LeaderAddr()
							if err != nil {
								resp.Error = err.Error()
							} else {
								resp.Leader = laddr
							}
						}
					}
				} else {
					resp.Error = "unauthorized"
				}
			}
			if err := marshalAndWrite(conn, resp); err != nil {
				return
			}
		}
	}
}

func marshalAndWrite(conn net.Conn, m pb.Message) error {
	p, err := pb.Marshal(m)
	if err != nil {
		conn.Close()
		return err
	}
	return writeBytesWithLength(conn, p)
}

func writeBytesWithLength(conn net.Conn, p []byte) error {
	b := make([]byte, protoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	_, err := conn.Write(b)
	if err != nil {
		return err
	}
	_, err = conn.Write(p)
	return err
}

// gzCompress compresses the given byte slice.
func gzCompress(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("gzip new writer: %s", err)
	}

	if _, err := gzw.Write(b); err != nil {
		return nil, fmt.Errorf("gzip Write: %s", err)
	}
	if err := gzw.Close(); err != nil {
		return nil, fmt.Errorf("gzip Close: %s", err)
	}
	return buf.Bytes(), nil
}
