package cluster

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/command"
)

var (
	// ErrNodeIDRequired is returned a join request doesn't supply a node ID
	ErrNodeIDRequired = errors.New("node required")

	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")

	// ErrNotifyFailed is returned when a node fails to notify another node
	ErrNotifyFailed = errors.New("failed to notify node")
)

// Joiner executes a node-join operation.
type Joiner struct {
	numAttempts     int
	attemptInterval time.Duration

	client *Client
	logger *log.Logger
}

// NewJoiner returns an instantiated Joiner.
func NewJoiner(client *Client, numAttempts int, attemptInterval time.Duration) *Joiner {
	return &Joiner{
		client:          client,
		numAttempts:     numAttempts,
		attemptInterval: attemptInterval,
		logger:          log.New(os.Stderr, "[cluster-join] ", log.LstdFlags),
	}
}

// Do makes the actual join request. If the join is successful with any address,
// that address is returned. Otherwise, an error is returned.
func (j *Joiner) Do(targetAddrs []string, id, addr string, voter bool) (string, error) {
	if id == "" {
		return "", ErrNodeIDRequired
	}

	var err error
	var joinee string
	for i := 0; i < j.numAttempts; i++ {
		for _, ta := range targetAddrs {
			joinee, err = j.join(ta, id, addr, voter)
			if err == nil {
				// Success!
				return joinee, nil
			}
			j.logger.Printf("failed to join via node at %s: %s", ta, err)
		}
		if i+1 < j.numAttempts {
			// This logic message only make sense if performing more than 1 join-attempt.
			j.logger.Printf("failed to join cluster at %s, sleeping %s before retry", targetAddrs, j.attemptInterval)
			time.Sleep(j.attemptInterval)
		}
	}
	j.logger.Printf("failed to join cluster at %s, after %d attempt(s)", targetAddrs, j.numAttempts)
	return "", ErrJoinFailed
}

func (j *Joiner) join(targetAddr, id, addr string, voter bool) (string, error) {
	req := &command.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   voter,
	}

	// Attempt to join.
	if err := j.client.Join(req, targetAddr, time.Second); err != nil {
		return "", err
	}
	return targetAddr, nil
}
