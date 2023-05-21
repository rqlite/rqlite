package cluster

import (
	"time"

	"github.com/rqlite/rqlite/command"
)

// Control is an interface for interacting with a cluster.
type Control interface {
	LeaderAddr() (string, error)
	WaitForRemoval(string, time.Duration) error
}

// Remover executes a node-removal operation.
type Remover struct {
	timeout time.Duration
	control Control
	client  *Client
}

// / NewRemover returns an instantiated Remover.
func NewRemover(client *Client, timeout time.Duration, control Control) *Remover {
	return &Remover{
		client:  client,
		timeout: timeout,
		control: control,
	}
}

// Do executes the node-removal operation.
func (r *Remover) Do(id string, confirm bool) error {
	laddr, err := r.control.LeaderAddr()
	if err != nil {
		return err
	}

	rn := &command.RemoveNodeRequest{
		Id: id,
	}

	if err := r.client.RemoveNode(rn, laddr, nil, r.timeout); err != nil {
		return err
	}

	if confirm {
		if err := r.control.WaitForRemoval(id, r.timeout); err != nil {
			return err
		}
	}

	return nil
}
