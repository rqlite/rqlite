package cluster

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
	initialPoolSize = 4
	maxPoolCapacity = 64
	maxRetries      = 8

	protoBufferLengthSize = 8
)

// CreateRaftDialer creates a dialer for connecting to other nodes' Raft service. If the cert and
// key arguments are not set, then the returned dialer will not use TLS.
func CreateRaftDialer(cert, key, caCert, serverName string, Insecure bool) (*tcp.Dialer, error) {
	var dialerTLSConfig *tls.Config
	var err error
	if cert != "" || key != "" {
		dialerTLSConfig, err = rtls.CreateClientConfig(cert, key, caCert, serverName, Insecure)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config for Raft dialer: %s", err.Error())
		}
	}
	return tcp.NewDialer(MuxRaftHeader, dialerTLSConfig), nil
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

	lMu           sync.RWMutex
	localNodeAddr string
	localServ     *Service

	mu            sync.RWMutex
	poolInitialSz int
	pools         map[string]pool.Pool
}

// NewClient returns a client instance for talking to a remote node.
// Clients will retry certain commands if they fail, to allow for
// remote node restarts. Cluster management operations such as joining
// and removing nodes are not retried, to make it clear to the operator
// that the operation failed. In addition, higher-level code will
// usually retry these operations.
func NewClient(dl Dialer, t time.Duration) *Client {
	return &Client{
		dialer:        dl,
		timeout:       t,
		poolInitialSz: initialPoolSize,
		pools:         make(map[string]pool.Pool),
	}
}

// SetLocal informs the client instance of the node address for the node
// using this client. Along with the Service instance it allows this
// client to serve requests for this node locally without the network hop.
func (c *Client) SetLocal(nodeAddr string, serv *Service) error {
	c.lMu.Lock()
	defer c.lMu.Unlock()
	c.localNodeAddr = nodeAddr
	c.localServ = serv
	return nil
}

// GetNodeAPIAddr retrieves the API Address for the node at nodeAddr
func (c *Client) GetNodeAPIAddr(nodeAddr string, timeout time.Duration) (string, error) {
	c.lMu.RLock()
	defer c.lMu.RUnlock()
	if c.localNodeAddr == nodeAddr && c.localServ != nil {
		// Serve it locally!
		stats.Add(numGetNodeAPIRequestLocal, 1)
		return c.localServ.GetNodeAPIURL(), nil
	}

	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_GET_NODE_API_URL,
	}
	p, err := c.retry(command, nodeAddr, timeout)
	if err != nil {
		return "", err
	}

	a := &proto.Address{}
	err = pb.Unmarshal(p, a)
	if err != nil {
		return "", fmt.Errorf("protobuf unmarshal: %w", err)
	}

	return a.Url, nil
}

// Execute performs an Execute on a remote node. If username is an empty string
// no credential information will be included in the Execute request to the
// remote node.
func (c *Client) Execute(er *command.ExecuteRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) ([]*command.ExecuteResult, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_EXECUTE,
		Request: &proto.Command_ExecuteRequest{
			ExecuteRequest: er,
		},
		Credentials: creds,
	}
	p, err := c.retry(command, nodeAddr, timeout)
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
	return a.Results, nil
}

// Query performs a Query on a remote node.
func (c *Client) Query(qr *command.QueryRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) ([]*command.QueryRows, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_QUERY,
		Request: &proto.Command_QueryRequest{
			QueryRequest: qr,
		},
		Credentials: creds,
	}
	p, err := c.retry(command, nodeAddr, timeout)
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

// Request performs an ExecuteQuery on a remote node.
func (c *Client) Request(r *command.ExecuteQueryRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) ([]*command.ExecuteQueryResponse, error) {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_REQUEST,
		Request: &proto.Command_ExecuteQueryRequest{
			ExecuteQueryRequest: r,
		},
		Credentials: creds,
	}
	p, err := c.retry(command, nodeAddr, timeout)
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

// Backup retrieves a backup from a remote node and writes to the io.Writer
func (c *Client) Backup(br *command.BackupRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration, w io.Writer) error {
	conn, err := c.dial(nodeAddr, c.timeout)
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
	ss := time.Now()
	_, err = io.Copy(w, rc)
	fmt.Println("backup copy time on client side:", time.Since(ss))
	return err
}

// Load loads a SQLite file into the database.
func (c *Client) Load(lr *command.LoadRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	command := &proto.Command{
		Type: proto.Command_COMMAND_TYPE_LOAD,
		Request: &proto.Command_LoadRequest{
			LoadRequest: lr,
		},
		Credentials: creds,
	}
	p, err := c.retry(command, nodeAddr, timeout)
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

// RemoveNode removes a node from the cluster
func (c *Client) RemoveNode(rn *command.RemoveNodeRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr, c.timeout)
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
func (c *Client) Notify(nr *command.NotifyRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr, c.timeout)
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
func (c *Client) Join(jr *command.JoinRequest, nodeAddr string, creds *proto.Credentials, timeout time.Duration) error {
	for {
		conn, err := c.dial(nodeAddr, c.timeout)
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
func (c *Client) Stats() (map[string]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := map[string]interface{}{
		"timeout":         c.timeout.String(),
		"local_node_addr": c.localNodeAddr,
	}

	if (len(c.pools)) == 0 {
		return stats, nil
	}

	poolStats := make(map[string]interface{}, len(c.pools))
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

func (c *Client) dial(nodeAddr string, timeout time.Duration) (net.Conn, error) {
	var pl pool.Pool
	var ok bool

	c.mu.RLock()
	pl, ok = c.pools[nodeAddr]
	c.mu.RUnlock()

	// Do we need a new pool for the given address?
	if !ok {
		if err := func() error {
			c.mu.Lock()
			defer c.mu.Unlock()
			pl, ok = c.pools[nodeAddr]
			if ok {
				return nil // Pool was inserted just after we checked.
			}

			// New pool is needed for given address.
			factory := func() (net.Conn, error) { return c.dialer.Dial(nodeAddr, c.timeout) }
			p, err := pool.NewChannelPool(c.poolInitialSz, maxPoolCapacity, factory)
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
func (c *Client) retry(command *proto.Command, nodeAddr string, timeout time.Duration) ([]byte, error) {
	var p []byte
	var errOuter error
	var nRetries int
	for {
		p, errOuter = func() ([]byte, error) {
			conn, errInner := c.dial(nodeAddr, c.timeout)
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
			return nil, errOuter
		}
	}
	return p, nil
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
		return fmt.Errorf("write length: %w", err)
	}
	// Write actual protobuf.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err = conn.Write(p)
	if err != nil {
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
		return nil, fmt.Errorf("read protobuf bytes: %w", err)
	}
	return p, nil
}

func handleConnError(conn net.Conn) {
	if pc, ok := conn.(*pool.Conn); ok {
		pc.MarkUnusable()
	}
}

func gzUncompress(b []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("unmarshal gzip NewReader: %w", err)
	}

	ub, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("unmarshal gzip ReadAll: %w", err)
	}

	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("unmarshal gzip Close: %w", err)
	}
	return ub, nil
}
