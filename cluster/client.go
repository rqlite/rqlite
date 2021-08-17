package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/rqlite/rqlite/command"
)

// Client allows communicating with a remote node.
type Client struct {
	dialer  Dialer
	timeout time.Duration
}

// NewClient returns a client instance for talking to a remote node.
func NewClient(dl Dialer) *Client {
	return &Client{
		dialer:  dl,
		timeout: 10 * time.Second,
	}
}

// GetNodeAPIAddr retrieves the API Address for the node at nodeAddr
func (c *Client) GetNodeAPIAddr(nodeAddr string) (string, error) {
	conn, err := c.dialer.Dial(nodeAddr, c.timeout)
	if err != nil {
		return "", fmt.Errorf("dial connection: %s", err)
	}
	defer conn.Close()

	// Send the request
	command := &Command{
		Type: Command_COMMAND_TYPE_GET_NODE_API_URL,
	}
	p, err := proto.Marshal(command)
	if err != nil {
		return "", fmt.Errorf("command marshal: %s", err)
	}

	// Write length of Protobuf, the Protobuf
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b[0:], uint16(len(p)))

	_, err = conn.Write(b)
	if err != nil {
		return "", fmt.Errorf("write protobuf length: %s", err)
	}
	_, err = conn.Write(p)
	if err != nil {
		return "", fmt.Errorf("write protobuf: %s", err)
	}

	b, err = ioutil.ReadAll(conn)
	if err != nil {
		return "", fmt.Errorf("read protobuf bytes: %s", err)
	}

	a := &Address{}
	err = proto.Unmarshal(b, a)
	if err != nil {
		return "", fmt.Errorf("protobuf unmarshal: %s", err)
	}

	return a.Url, nil
}

// Execute performs an Execute on a remote node.
func (c *Client) Execute(nodeAddr string, er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
	conn, err := c.dialer.Dial(nodeAddr, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("dial connection: %s", err)
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_EXECUTE,
		Request: &Command_ExecuteRequest{
			ExecuteRequest: er,
		},
	}
	p, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("command marshal: %s", err)
	}

	// Write length of Protobuf, the Protobuf
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b[0:], uint16(len(p)))

	_, err = conn.Write(b)
	if err != nil {
		return nil, fmt.Errorf("write protobuf length: %s", err)
	}
	_, err = conn.Write(p)
	if err != nil {
		return nil, fmt.Errorf("write protobuf: %s", err)
	}

	b, err = ioutil.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("read protobuf bytes: %s", err)
	}

	a := &CommandExecuteResponse{}
	err = proto.Unmarshal(b, a)
	if err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %s", err)
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Results, nil
}

// Query performs an Query on a remote node.
func (c *Client) Query(nodeAddr string, qr *command.QueryRequest) ([]*command.QueryRows, error) {
	conn, err := c.dialer.Dial(nodeAddr, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("dial connection: %s", err)
	}
	defer conn.Close()

	// Create the request.
	command := &Command{
		Type: Command_COMMAND_TYPE_QUERY,
		Request: &Command_QueryRequest{
			QueryRequest: qr,
		},
	}
	p, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("command marshal: %s", err)
	}

	// Write length of Protobuf, the Protobuf
	b := make([]byte, 4)
	binary.LittleEndian.PutUint16(b[0:], uint16(len(p)))

	_, err = conn.Write(b)
	if err != nil {
		return nil, fmt.Errorf("write protobuf length: %s", err)
	}
	_, err = conn.Write(p)
	if err != nil {
		return nil, fmt.Errorf("write protobuf: %s", err)
	}

	b, err = ioutil.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("read protobuf bytes: %s", err)
	}

	a := &CommandQueryResponse{}
	err = proto.Unmarshal(b, a)
	if err != nil {
		return nil, fmt.Errorf("protobuf unmarshal: %s", err)
	}

	if a.Error != "" {
		return nil, errors.New(a.Error)
	}
	return a.Rows, nil
}

// Stats returns stats on the Client instance
func (c *Client) Stats() (map[string]interface{}, error) {
	return map[string]interface{}{
		"timeout": c.timeout,
	}, nil
}
