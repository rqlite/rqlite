package cluster

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/rqlite/rqlite/v8/cluster/proto"
	command "github.com/rqlite/rqlite/v8/command/proto"
)

var (
	// ErrNodeIDRequired is returned a join request doesn't supply a node ID
	ErrNodeIDRequired = errors.New("node required")

	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")

	// ErrJoinCanceled is returned when a join operation is canceled
	ErrJoinCanceled = errors.New("join operation canceled")

	// ErrNotifyFailed is returned when a node fails to notify another node
	ErrNotifyFailed = errors.New("failed to notify node")
)

// Joiner executes a node-join operation.
type Joiner struct {
	numAttempts     int
	attemptInterval time.Duration

	client *Client
	creds  *proto.Credentials
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

// SetCredentials sets the credentials for the Joiner.
func (j *Joiner) SetCredentials(creds *proto.Credentials) {
	j.creds = creds
}

// Do makes the actual join request. If the join is successful with any address,
// that address is returned. Otherwise, an error is returned.
func (j *Joiner) Do(ctx context.Context, targetAddrs []string, id, addr string, suf Suffrage) (string, error) {
	if id == "" {
		return "", ErrNodeIDRequired
	}

	var err error
	var joinee string
	for i := 0; i < j.numAttempts; i++ {
		for _, ta := range targetAddrs {
			select {
			case <-ctx.Done():
				return "", ErrJoinCanceled
			default:
				joinee, err = j.join(ta, id, addr, suf)
				if err == nil {
					return joinee, nil
				}
				helpMsg := ""
				if j.creds == nil {
					helpMsg = " (did you forget to set -join-as?)"
				}
				j.logger.Printf("failed to join via node at %s: %s%s", ta, err, helpMsg)
			}
		}
		if i+1 < j.numAttempts {
			// This logic message only make sense if performing more than 1 join-attempt.
			j.logger.Printf("failed to join cluster at %s, sleeping %s before retry", targetAddrs, j.attemptInterval)
			select {
			case <-ctx.Done():
				return "", ErrJoinCanceled
			case <-time.After(j.attemptInterval):
				continue // Proceed with the next attempt
			}
		}
	}
	j.logger.Printf("failed to join cluster at %s, after %d attempt(s)", targetAddrs, j.numAttempts)
	return "", ErrJoinFailed
}

func (j *Joiner) join(targetAddr, id, addr string, suf Suffrage) (string, error) {
	req := &command.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   suf.IsVoter(),
	}

	// Attempt to join.
	if err := j.client.Join(req, targetAddr, j.creds, time.Second); err != nil {
		return "", err
	}
	return targetAddr, nil
}
