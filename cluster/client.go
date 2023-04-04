package cluster

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/tcp/pool"
	"google.golang.org/protobuf/proto"
)

const (
	initialPoolSize = 4
	maxPoolCapacity = 64

	protoBufferLengthSize = 8
)

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

	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// Send the request
	command := &Command{
		Type: Command_COMMAND_TYPE_GET_NODE_API_URL,
	}
	if err := writeCommand(conn, command, timeout); err != nil {
		handleConnError(conn)
		return "", err
	}

	p, err := readResponse(conn, timeout)
	if err != nil {
		handleConnError(conn)
		return "", err
	}

	a := &Address{}
	err = proto.Unmarshal(p, a)
	if err != nil {
		return "", fmt.Errorf("protobuf unmarshal: %s", err)
	}

	return a.Url, nil
}

// Execute performs an Execute on a remote node. If username is an empty string
// no credential information will be included in the Execute request to the
// remote node.
func (c *Client) Execute(er *command.ExecuteRequest, nodeAddr string, creds *Credentials, timeout time.Duration) ([]*command.ExecuteResult, error) {
	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_EXECUTE,
		Request: &Command_ExecuteRequest{
			ExecuteRequest: er,
		},
		Credentials: creds,
	}
	if err := writeCommand(conn, command, timeout); err != nil {
		handleConnError(conn)
		return nil, err
	}

	p, err := readResponse(conn, timeout)
	if err != nil {
		handleConnError(conn)
		return nil, err
	}

	a := &CommandExecuteResponse{}
	err = proto.Unmarshal(p, a)
	if err != nil {
		return nil, err
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Results, nil
}

// Query performs a Query on a remote node.
func (c *Client) Query(qr *command.QueryRequest, nodeAddr string, creds *Credentials, timeout time.Duration) ([]*command.QueryRows, error) {
	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_QUERY,
		Request: &Command_QueryRequest{
			QueryRequest: qr,
		},
		Credentials: creds,
	}
	if err := writeCommand(conn, command, timeout); err != nil {
		handleConnError(conn)
		return nil, err
	}

	p, err := readResponse(conn, timeout)
	if err != nil {
		handleConnError(conn)
		return nil, err
	}

	a := &CommandQueryResponse{}
	err = proto.Unmarshal(p, a)
	if err != nil {
		return nil, err
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Rows, nil
}

// Backup retrieves a backup from a remote node and writes to the io.Writer
func (c *Client) Backup(br *command.BackupRequest, nodeAddr string, creds *Credentials, timeout time.Duration, w io.Writer) error {
	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send the request
	command := &Command{
		Type: Command_COMMAND_TYPE_BACKUP,
		Request: &Command_BackupRequest{
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

	// Decompress....
	p, err = gzUncompress(p)
	if err != nil {
		handleConnError(conn)
		return fmt.Errorf("backup decompress: %s", err)
	}

	resp := &CommandBackupResponse{}
	err = proto.Unmarshal(p, resp)
	if err != nil {
		return fmt.Errorf("backup unmarshal: %s", err)
	}

	if resp.Error != "" {
		return errors.New(resp.Error)
	}

	if _, err := w.Write(resp.Data); err != nil {
		return fmt.Errorf("backup write: %s", err)
	}
	return nil
}

// Load loads a SQLite file into the database.
func (c *Client) Load(lr *command.LoadRequest, nodeAddr string, creds *Credentials, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_LOAD,
		Request: &Command_LoadRequest{
			LoadRequest: lr,
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

	a := &CommandLoadResponse{}
	err = proto.Unmarshal(p, a)
	if err != nil {
		return err
	}

	if a.Error != "" {
		return errors.New(a.Error)
	}
	return nil
}

// RemoveNode removes a node from the cluster
func (c *Client) RemoveNode(rn *command.RemoveNodeRequest, nodeAddr string, creds *Credentials, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_REMOVE_NODE,
		Request: &Command_RemoveNodeRequest{
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

	a := &CommandRemoveNodeResponse{}
	err = proto.Unmarshal(p, a)
	if err != nil {
		return err
	}

	if a.Error != "" {
		return errors.New(a.Error)
	}
	return nil
}

// Notify notifies a remote node that this node is ready to bootstrap.
func (c *Client) Notify(nr *command.NotifyRequest, nodeAddr string, timeout time.Duration) error {
	conn, err := c.dial(nodeAddr, c.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_NOTIFY,
		Request: &Command_NotifyRequest{
			NotifyRequest: nr,
		},
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

	a := &CommandNotifyResponse{}
	err = proto.Unmarshal(p, a)
	if err != nil {
		return err
	}

	if a.Error != "" {
		return errors.New(a.Error)
	}
	return nil
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
		return nil, fmt.Errorf("pool get: %s", err)
	}
	return conn, nil
}

func writeCommand(conn net.Conn, c *Command, timeout time.Duration) error {
	p, err := proto.Marshal(c)
	if err != nil {
		return fmt.Errorf("command marshal: %s", err)
	}

	// Write length of Protobuf
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	b := make([]byte, protoBufferLengthSize)
	binary.LittleEndian.PutUint64(b[0:], uint64(len(p)))
	_, err = conn.Write(b)
	if err != nil {
		return fmt.Errorf("write length: %s", err)
	}
	// Write actual protobuf.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err = conn.Write(p)
	if err != nil {
		return fmt.Errorf("write protobuf bytes: %s", err)
	}
	return nil
}

func readResponse(conn net.Conn, timeout time.Duration) ([]byte, error) {
	// Read length of incoming response.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	b := make([]byte, protoBufferLengthSize)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		return nil, fmt.Errorf("read protobuf length: %s", err)
	}
	sz := binary.LittleEndian.Uint64(b[0:])

	// Read in the actual response.
	p := make([]byte, sz)
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	_, err = io.ReadFull(conn, p)
	if err != nil {
		return nil, fmt.Errorf("read protobuf bytes: %s", err)
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
		return nil, fmt.Errorf("unmarshal gzip NewReader: %s", err)
	}

	ub, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("unmarshal gzip ReadAll: %s", err)
	}

	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("unmarshal gzip Close: %s", err)
	}
	return ub, nil
}
