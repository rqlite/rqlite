package cluster

import (
	"compress/gzip"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v8/auth"
	"github.com/rqlite/rqlite/v8/cluster/proto"
	command "github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/rtls"
	"github.com/rqlite/rqlite/v8/tcp"
	"github.com/rqlite/rqlite/v8/tcp/pool"
	pb "google.golang.org/protobuf/proto"
)

const (
	maxPoolCapacity   = 64
	defaultMaxRetries = 0
	noRetries         = 0

	protoBufferLengthSize = 8
)

// CreateRaftDialer creates a dialer for connecting to other nodes' Raft service. If the cert and
// key arguments are not set, then the returned dialer will not use TLS. If they are set then
// the dialer will use TLS. A started CertMonitor will also be returned. The caller is responsible
// for stopping the CertMonitor when the Dialer is no longer needed. The serverName argument is used
// to validate the server certificate. If Insecure is true, then the dialer will not validate the
// server certificate.
func CreateRaftDialer(cert, key, caCert, serverName string, Insecure bool) (*tcp.Dialer, *rtls.CertMonitor, error) {
	var dialerTLSConfig *tls.Config
	var err error
	var cm *rtls.CertMonitor
	if cert != "" || key != "" {
		cm, err = rtls.NewCertMonitor(cert, key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create TLS config for Raft dialer: %s", err.Error())
		}
		dialerTLSConfig, err = rtls.CreateClientConfigWithFunc(cm.GetCertificate, caCert, serverName, Insecure)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create TLS config for Raft dialer: %s", err.Error())
		}
		cm.Start()
	}
	return tcp.NewDialer(MuxRaftHeader, dialerTLSConfig), cm, nil
}

// CredentialsFor returns a Credentials instance for the given username, or nil if
// the given CredentialsStore is nil, or the username is not found.
func CredentialsFor(credStr *auth.CredentialsStore, username string) *proto.Credentials {
	if credStr == nil {
		return nil
	}
	pw, ok := credStr.Password(username)
	if !ok {
		return nil
	}
	return &proto.Credentials{
		Username: username,
		Password: pw,
	}
}

// Client allows communicating with a remote node.
type Client struct {
	dialer  Dialer
	timeout time.Duration

	localMu       sync.RWMutex
	localNodeAddr string
	localServ     *Service
	localVersion  string

	poolMu sync.RWMutex
	pools  map[string]pool.Pool
}

// NewClient returns a client instance for talking to a remote node.
// Clients will retry certain commands if they fail, to allow for
// remote node restarts. Cluster management operations such as joining
// and removing nodes are not retried, to make it clear to the operator
// that the operation failed. In addition, higher-level code will
// usually retry these operations.
func NewClient(dl Dialer, t time.Duration) *Client {
	return &Client{
		dialer:  dl,
		timeout: t,
		pools:   make(map[string]pool.Pool),
	}
}

// SetLocal informs the client instance of the node address for the node
// using this client. Along with the Service instance it allows this
// client to serve requests for this node locally without the network hop.
func (c *Client) SetLocal(nodeAddr string, serv *Service) error {
	c.localMu.Lock()
	defer c.localMu.Unlock()
	c.localNodeAddr = nodeAddr
	c.localServ = serv
	return nil
}

// SetLocalVersion informs the client instance of the version of the software
// running on this node. This is used so the client can serve this information
// quickly.
func (c *Client) SetLocalVersion(version string) error {
	c.localMu.Lock()
	defer c.localMu.Unlock()
	c.localVersion = version
	return nil
}

// GetLocalNodeAddr retrieves the version of software of the software
// running on this node.
func (c *Client) GetLocalVersion() string {
	c.localMu.RLock()
	defer c.localMu.RUnlock()
	return c.localVersion
}

// GetNodeAPIAddr retrieves metadata for the node at nodeAddr
func (c *Client) GetNodeMeta(nodeAddr string, retries int, timeout time.Duration) (*proto.NodeMeta, error) {
	c.localMu.RLock()
	defer c.localMu.RUnlock()
	if c.localNodeAddr == nodeAddr && c.localServ != nil {
		// Serve it locally!
		stats.Add(numGetNodeAPIRequestLocal, 1)
		return &proto.NodeMeta{
			Url:     c.localServ.GetNodeAPIURL(),
			Version: c.GetLocalVersion(),
		}, nil
	}

	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_GET_NODE_META,
	}
	p, nr, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numGetNodeAPIRequestRetries, int64(nr))
	if err != nil {
		return nil, err
	}

	a := &proto.NodeMeta{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	if a.Version == "" {
		// Handle nodes running older code.
		a.Version = "unknown"
	}
	return a, nil
}

// GetCommitIndex retrieves the commit index for the node at nodeAddr
func (c *Client) GetCommitIndex(nodeAddr string, retries int, timeout time.Duration) (uint64, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_GET_NODE_META,
	}
	p, nr, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numGetNodeAPIRequestRetries, int64(nr))
	if err != nil {
		return 0, err
	}

	a := &proto.NodeMeta{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return 0, fmt.Errorf("protobuf unmarshal: %w", err)
	}
	return a.CommitIndex, nil
}

// Execute performs an Execute on a remote node. If creds is nil, then
// no credential information will be included in the Execute request to the
// remote node.
func (c *Client) Execute(er *command.ExecuteRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration, retries int) ([]*command.ExecuteQueryResponse, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_EXECUTE,
		Request: &proto.Command_ExecuteRequest{
			ExecuteRequest: er,
		},
		Credentials: creds,
	}
	p, nr, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientExecuteRetries, int64(nr))
	if err != nil {
		return nil, err
	}

	a := &proto.CommandExecuteResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return nil, err
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Response, nil
}

// Query performs a Query on a remote node. If creds is nil, then
// no credential information will be included in the Query request to the
// remote node.
func (c *Client) Query(qr *command.QueryRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) ([]*command.QueryRows, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_QUERY,
		Request: &proto.Command_QueryRequest{
			QueryRequest: qr,
		},
		Credentials: creds,
	}
	p, nr, err := c.retry(command, nodeAddr, timeout, defaultMaxRetries)
	stats.Add(numClientQueryRetries, int64(nr))
	if err != nil {
		return nil, err
	}

	a := &proto.CommandQueryResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return nil, err
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Rows, nil
}

// Request performs an ExecuteQuery on a remote node. If creds is nil, then
// no credential information will be included in the ExecuteQuery request to the
// remote node.
func (c *Client) Request(r *command.ExecuteQueryRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration, retries int) ([]*command.ExecuteQueryResponse, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_REQUEST,
		Request: &proto.Command_ExecuteQueryRequest{
			ExecuteQueryRequest: r,
		},
		Credentials: creds,
	}
	p, nr, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientRequestRetries, int64(nr))
	if err != nil {
		return nil, err
	}

	a := &proto.CommandRequestResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return nil, err
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Response, nil
}

// Backup retrieves a backup from a remote node and writes to the io.Writer.
// If creds is nil, then no credential information will be included in the
// Backup request to the remote node.
func (c *Client) Backup(br *command.BackupRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration, w io.Writer) error {
	conn, err := c.dial(nodeAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_BACKUP_STREAM,
		Request: &proto.Command_BackupRequest{
			BackupRequest: br,
		},
		Credentials: creds,
	}

	if err := writeCommand(conn, command, timeout); err != nil {
		handleConnError(conn)
		return err
	}

	p, err := readResponse(conn, timeout)
	if err != nil {
		handleConnError(conn)
		return err
	}

	a := &proto.CommandBackupResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return err
	}
	if a.Error != "" {
		return errors.New(a.Error)
	}

	// The backup stream is unconditionally compressed, so depending on whether
	// the user requested compression, we may need to decompress the response.
	var rc io.ReadCloser
	rc = conn
	if !br.Compress {
		gzr, err := gzip.NewReader(conn)
		if err != nil {
			return err
		}
		gzr.Multistream(false)
		rc = gzr
		defer rc.Close()
	}
	_, err = io.Copy(w, rc)
	return err
}

// Load loads a SQLite file into the database. If creds is nil, then no
// credential information will be included in the Load request to the remote node.
func (c *Client) Load(lr *command.LoadRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration, retries int) error {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_LOAD,
		Request: &proto.Command_LoadRequest{
			LoadRequest: lr,
		},
		Credentials: creds,
	}
	p, nr, err := c.retry(command, nodeAddr, timeout, retries)
	stats.Add(numClientLoadRetries, int64(nr))
	if err != nil {
		return err
	}

	a := &proto.CommandLoadResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return err
	}

	if a.Error != "" {
		return errors.New(a.Error)
	}
	return nil
}

// RemoveNode removes a node from the cluster. If creds is nil, then no
// credential information will be included in the RemoveNode request to the
// remote node.
func (c *Client) RemoveNode(rn *command.RemoveNodeRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create the request.
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_REMOVE_NODE,
		Request: &proto.Command_RemoveNodeRequest{
			RemoveNodeRequest: rn,
		},
		Credentials: creds,
	}
	if err := writeCommand(conn, command, timeout); err != nil {
		handleConnError(conn)
		return err
	}

	p, err := readResponse(conn, timeout)
	if err != nil {
		handleConnError(conn)
		return err
	}

	a := &proto.CommandRemoveNodeResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return err
	}

	if a.Error != "" {
		return errors.New(a.Error)
	}
	return nil
}

// Notify notifies a remote node that this node is ready to bootstrap.
// If creds is nil, then no credential information will be included in
// // the Notify request to the remote node.
func (c *Client) Notify(nr *command.NotifyRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create the request.
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_NOTIFY,
		Request: &proto.Command_NotifyRequest{
			NotifyRequest: nr,
		},
		Credentials: creds,
	}
	if err := writeCommand(conn, command, timeout); err != nil {
		handleConnError(conn)
		return err
	}

	p, err := readResponse(conn, timeout)
	if err != nil {
		handleConnError(conn)
		return err
	}

	a := &proto.CommandNotifyResponse{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return err
	}

	if a.Error != "" {
		return errors.New(a.Error)
	}
	return nil
}

// Join joins this node to a cluster at the remote address nodeAddr.
// If creds is nil, then no credential information will be included in
// the Join request to the remote node.
func (c *Client) Join(jr *command.JoinRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	for {
		conn, err := c.dial(nodeAddr)
		if err != nil {
			return err
		}
		defer conn.Close()

		// Create the request.
		command := &proto.Command{
			Type: proto.Command_COMMAND_TYPE_JOIN,
			Request: &proto.Command_JoinRequest{
				JoinRequest: jr,
			},
			Credentials: creds,
		}

		if err := writeCommand(conn, command, timeout); err != nil {
			handleConnError(conn)
			return err
		}

		p, err := readResponse(conn, timeout)
		if err != nil {
			handleConnError(conn)
			return err
		}

		a := &proto.CommandJoinResponse{}
		err = pb.Unmarshal(p, a)
		if err != nil {
			return err
		}

		if a.Error != "" {
			if a.Error == "not leader" {
				if a.Leader == "" {
					return errors.New("no leader")
				}
				nodeAddr = a.Leader
				continue
			}
			return errors.New(a.Error)
		}
		return nil
	}
}

// Stats returns stats on the Client instance
func (c *Client) Stats() (map[string]any, error) {
	c.poolMu.RLock()
	defer c.poolMu.RUnlock()

	stats := map[string]any{
		"timeout":         c.timeout.String(),
		"local_node_addr": c.localNodeAddr,
	}

	if (len(c.pools)) == 0 {
		return stats, nil
	}

	poolStats := make(map[string]any, len(c.pools))
	for k, v := range c.pools {
		s, err := v.Stats()
		if err != nil {
			return nil, err
		}
		poolStats[k] = s
	}
	stats["conn_pool_stats"] = poolStats
	return stats, nil
}

func (c *Client) dial(nodeAddr string) (net.Conn, error) {
	var pl pool.Pool
	var ok bool

	c.poolMu.RLock()
	pl, ok = c.pools[nodeAddr]
	c.poolMu.RUnlock()

	// Do we need a new pool for the given address?
	if !ok {
		if err := func() error {
			c.poolMu.Lock()
			defer c.poolMu.Unlock()
			pl, ok = c.pools[nodeAddr]
			if ok {
				return nil // Pool was inserted just after we checked.
			}

			// New pool is needed for given address.
			factory := func() (net.Conn, error) { return c.dialer.Dial(nodeAddr, c.timeout) }
			p, err := pool.NewChannelPool(maxPoolCapacity, factory)
			if err != nil {
				return err
			}
			c.pools[nodeAddr] = p
			pl = p
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	// Got pool, now get a connection.
	conn, err := pl.Get()
	if err != nil {
		return nil, fmt.Errorf("pool get: %w", err)
	}
	return conn, nil
}

// retry retries a command on a remote node. It does this so we churn through connections
// in the pool if we hit an error, as the remote node may have restarted and the pool's
// connections are now stale.
func (c *Client) retry(command *proto.Command, nodeAddr string, timeout time.Duration, maxRetries int) ([]byte, int, error) {
	var p []byte
	var errOuter error
	var nRetries int
	for {
		p, errOuter = func() ([]byte, error) {
			conn, errInner := c.dial(nodeAddr)
			if errInner != nil {
				return nil, errInner
			}
			defer conn.Close()

			if errInner = writeCommand(conn, command, timeout); errInner != nil {
				handleConnError(conn)
				return nil, errInner
			}

			b, errInner := readResponse(conn, timeout)
			if errInner != nil {
				handleConnError(conn)
				return nil, errInner
			}
			return b, nil
		}()
		if errOuter == nil {
			break
		}
		nRetries++
		stats.Add(numClientRetries, 1)
		if nRetries > maxRetries {
			return nil, nRetries, errOuter
		}
	}
	return p, nRetries, nil
}

func writeCommand(conn net.Conn, c *proto.Command, timeout time.Duration) error {
	p, err := pb.Marshal(c)
	if err != nil {
		return fmt.Errorf("command marshal: %w", err)
	}

	// Write length of Protobuf
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	b := make([]byte, protoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	_, err = conn.Write(b)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientWriteTimeouts, 1)
		}
		return fmt.Errorf("write length: %w", err)
	}
	// Write actual protobuf.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err = conn.Write(p)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientWriteTimeouts, 1)
		}
		return fmt.Errorf("write protobuf bytes: %w", err)
	}
	return nil
}

func readResponse(conn net.Conn, timeout time.Duration) (buf []byte, retErr error) {
	defer func() {
		// Connecting to an open port, but not a rqlite Raft API, may cause a panic
		// when the system tries to read the response. This is a workaround.
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic reading response from node: %v", r)
		}
	}()

	// Read length of incoming response.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	b := make([]byte, protoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientReadTimeouts, 1)
		}
		return nil, fmt.Errorf("read protobuf length: %w", err)
	}
	sz := binary.LittleEndian.Uint64(b[0:])

	// Read in the actual response.
	p := make([]byte, sz)
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	_, err = io.ReadFull(conn, p)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			stats.Add(numClientReadTimeouts, 1)
		}
		return nil, fmt.Errorf("read protobuf bytes: %w", err)
	}
	return p, nil
}

func handleConnError(conn net.Conn) {
	if pc, ok := conn.(*pool.Conn); ok {
		pc.MarkUnusable()
	}
}
