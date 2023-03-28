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

	"github.com/rqlite/rqlite/auth"
	"github.com/rqlite/rqlite/command"
	"google.golang.org/protobuf/proto"
)

// stats captures stats for the Cluster service.
var stats *expvar.Map

const (
	numGetNodeAPIRequest  = "num_get_node_api_req"
	numGetNodeAPIResponse = "num_get_node_api_resp"
	numExecuteRequest     = "num_execute_req"
	numQueryRequest       = "num_query_req"
	numBackupRequest      = "num_backup_req"
	numLoadRequest        = "num_load_req"
	numRemoveNodeRequest  = "num_remove_node_req"

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
	stats.Add(numBackupRequest, 0)
	stats.Add(numLoadRequest, 0)
	stats.Add(numRemoveNodeRequest, 0)
	stats.Add(numGetNodeAPIRequestLocal, 0)
}

// Dialer is the interface dialers must implement.
type Dialer interface {
	// Dial is used to create a connection to a service listening
	// on an address.
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Database is the interface any queryable system must implement
type Database interface {
	// Execute executes a slice of queries, none of which is expected
	// to return rows.
	Execute(er *command.ExecuteRequest) ([]*command.ExecuteResult, error)

	// Query executes a slice of queries, each of which returns rows.
	Query(qr *command.QueryRequest) ([]*command.QueryRows, error)

	// Backup writes a backup of the database to the writer.
	Backup(br *command.BackupRequest, dst io.Writer) error

	// Loads an entire SQLite file into the database
	Load(lr *command.LoadRequest) error
}

// Manager is the interface node-management systems must implement
type Manager interface {
	// Remove removes the node, given by id, from the cluster
	Remove(rn *command.RemoveNodeRequest) error
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// AA authenticates and checks authorization for the given perm.
	AA(username, password, perm string) bool
}

// Transport is the interface the network layer must provide.
type Transport interface {
	net.Listener
	Dialer
}

// Service provides information about the node and cluster.
type Service struct {
	tn   Transport // Network layer this service uses
	addr net.Addr  // Address on which this service is listening

	db  Database // The queryable system.
	mgr Manager  // The cluster management system.

	credentialStore CredentialStore

	mu      sync.RWMutex
	https   bool   // Serving HTTPS?
	apiAddr string // host:port this node serves the HTTP API.

	logger *log.Logger
}

// New returns a new instance of the cluster service
func New(tn Transport, db Database, m Manager, credentialStore CredentialStore) *Service {
	return &Service{
		tn:              tn,
		addr:            tn.Addr(),
		db:              db,
		mgr:             m,
		logger:          log.New(os.Stderr, "[cluster] ", log.LstdFlags),
		credentialStore: credentialStore,
	}
}

// Open opens the Service.
func (s *Service) Open() error {
	go s.serve()
	s.logger.Println("service listening on", s.tn.Addr())
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.tn.Close()
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
		conn, err := s.tn.Accept()
		if err != nil {
			return err
		}

		go s.handleConn(conn)
	}
}

func (s *Service) checkCommandPerm(c *Command, perm string) bool {
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

func (s *Service) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		b := make([]byte, protoBufferLengthSize)
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

		c := &Command{}
		err = proto.Unmarshal(p, c)
		if err != nil {
			conn.Close()
		}

		switch c.Type {
		case Command_COMMAND_TYPE_GET_NODE_API_URL:
			stats.Add(numGetNodeAPIRequest, 1)
			p, err = proto.Marshal(&Address{
				Url: s.GetNodeAPIURL(),
			})
			if err != nil {
				conn.Close()
			}
			writeBytesWithLength(conn, p)
			stats.Add(numGetNodeAPIResponse, 1)

		case Command_COMMAND_TYPE_EXECUTE:
			stats.Add(numExecuteRequest, 1)

			resp := &CommandExecuteResponse{}
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
					resp.Results = make([]*command.ExecuteResult, len(res))
					copy(resp.Results, res)
				}
			}

			p, err := proto.Marshal(resp)
			if err != nil {
				return
			}
			writeBytesWithLength(conn, p)

		case Command_COMMAND_TYPE_QUERY:
			stats.Add(numQueryRequest, 1)

			resp := &CommandQueryResponse{}

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

			p, err = proto.Marshal(resp)
			if err != nil {
				return
			}
			writeBytesWithLength(conn, p)

		case Command_COMMAND_TYPE_BACKUP:
			stats.Add(numBackupRequest, 1)

			resp := &CommandBackupResponse{}

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
			p, err = proto.Marshal(resp)
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
			writeBytesWithLength(conn, p)

		case Command_COMMAND_TYPE_LOAD:
			stats.Add(numLoadRequest, 1)

			resp := &CommandLoadResponse{}

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

			p, err = proto.Marshal(resp)
			if err != nil {
				return
			}
			writeBytesWithLength(conn, p)

		case Command_COMMAND_TYPE_REMOVE_NODE:
			stats.Add(numRemoveNodeRequest, 1)
			resp := &CommandRemoveNodeResponse{}

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

			p, err = proto.Marshal(resp)
			if err != nil {
				conn.Close()
			}
			writeBytesWithLength(conn, p)
		}
	}
}

func writeBytesWithLength(conn net.Conn, p []byte) {
	b := make([]byte, protoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	conn.Write(b)
	conn.Write(p)
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
