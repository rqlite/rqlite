package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rqlite/rqlite/command"
)

var (
	// ErrInvalidRedirect is returned when a node returns an invalid HTTP redirect.
	ErrInvalidRedirect = errors.New("invalid redirect received")

	// ErrNodeIDRequired is returned a join request doesn't supply a node ID
	ErrNodeIDRequired = errors.New("node required")

	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")

	// ErrNotifyFailed is returned when a node fails to notify another node
	ErrNotifyFailed = errors.New("failed to notify node")

	// ErrNodeClientRequired is returned when a node client is required, but not supplied
	ErrNodeClientRequired = errors.New("node client required")
)

// Joiner executes a node-join operation.
type Joiner struct {
	srcIP           string
	numAttempts     int
	attemptInterval time.Duration
	httpTLSCfg      *tls.Config

	username string
	password string

	httpClient *http.Client
	nodeClient *Client

	logger *log.Logger
}

// NewJoiner returns an instantiated Joiner.
func NewJoiner(srcIP string, numAttempts int, attemptInterval time.Duration, httpTLSCfg *tls.Config, nodeClient *Client) *Joiner {
	if httpTLSCfg == nil {
		httpTLSCfg = &tls.Config{InsecureSkipVerify: true}
	}

	// Source IP is optional
	dialer := &net.Dialer{}
	if srcIP != "" {
		netAddr := &net.TCPAddr{
			IP:   net.ParseIP(srcIP),
			Port: 0,
		}
		dialer = &net.Dialer{LocalAddr: netAddr}
	}

	joiner := &Joiner{
		srcIP:           srcIP,
		numAttempts:     numAttempts,
		attemptInterval: attemptInterval,
		httpTLSCfg:      httpTLSCfg,
		nodeClient:      nodeClient,
		logger:          log.New(os.Stderr, "[cluster-join] ", log.LstdFlags),
	}

	// Create and configure the client to connect to the other node.
	tr := &http.Transport{
		TLSClientConfig:   joiner.httpTLSCfg,
		Dial:              dialer.Dial,
		ForceAttemptHTTP2: true,
	}
	joiner.httpClient = &http.Client{Transport: tr}
	joiner.httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return joiner
}

// SetBasicAuth sets Basic Auth credentials for any join attempt.
func (j *Joiner) SetBasicAuth(username, password string) {
	j.username, j.password = username, password
}

// Do makes the actual join request. If any of the join addresses do not contain a
// protocol, both http:// and https:// are tried for that address. If the join is successful
// with any address, the Join URL of the node that joined is returned. Otherwise, an error
// is returned.
func (j *Joiner) Do(joinAddrs []string, id, addr string, voter bool) (string, error) {
	if id == "" {
		return "", ErrNodeIDRequired
	}

	var err error
	var joinee string
	for i := 0; i < j.numAttempts; i++ {
		for _, a := range normalizeAddrs(joinAddrs) {
			joinee, err = j.join(a, id, addr, voter)
			if err == nil {
				// Success!
				return joinee, nil
			} else {
				j.logger.Printf("failed to join via node at %s: %s", a, err)
			}
		}
		if i+1 < j.numAttempts {
			// This logic message only make sense if performing more than 1 join-attempt.
			j.logger.Printf("failed to join cluster at %s, sleeping %s before retry", joinAddrs, j.attemptInterval)
			time.Sleep(j.attemptInterval)
		}
	}
	j.logger.Printf("failed to join cluster at %s, after %d attempt(s)", joinAddrs, j.numAttempts)
	return "", ErrJoinFailed
}

func (j *Joiner) join(joinAddr, id, addr string, voter bool) (string, error) {
	if strings.HasPrefix(joinAddr, "raft") {
		return j.joinRaft(joinAddr, id, addr, voter)
	} else if strings.HasPrefix(joinAddr, "http") || strings.HasPrefix(joinAddr, "https") {
		return j.joinHTTP(joinAddr, id, addr, voter)
	}
	return "", fmt.Errorf("invalid join protocol: %s", joinAddr)
}

func (j *Joiner) joinHTTP(joinAddr, id, addr string, voter bool) (string, error) {
	fullAddr := fmt.Sprintf("%s/join", joinAddr)
	reqBody, err := json.Marshal(map[string]interface{}{
		"id":    id,
		"addr":  addr,
		"voter": voter,
	})
	if err != nil {
		return "", err
	}

	for {
		// Attempt to join.
		req, err := http.NewRequest("POST", fullAddr, bytes.NewReader(reqBody))
		if err != nil {
			return "", err
		}
		if j.username != "" && j.password != "" {
			req.SetBasicAuth(j.username, j.password)
		}

		var resp *http.Response
		var respB []byte
		err = func() error {
			req.Header.Add("Content-Type", "application/json")
			resp, err = j.httpClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			// Only significant in the event of an error response
			// from the remote node.
			respB, err = io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return "", err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			return fullAddr, nil
		case http.StatusMovedPermanently:
			fullAddr = resp.Header.Get("location")
			if fullAddr == "" {
				return "", ErrInvalidRedirect
			}
			continue
		default:
			return "", fmt.Errorf("%s: (%s)", resp.Status, string(respB))
		}
	}
}

func (j *Joiner) joinRaft(joinAddr, id, addr string, voter bool) (string, error) {
	if j.nodeClient == nil {
		return "", ErrNodeClientRequired
	}
	u, err := url.Parse(joinAddr)
	if err != nil {
		return "", err
	}

	jr := &command.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   voter,
	}
	return joinAddr, j.nodeClient.Join(jr, u.Host, 10*time.Second)
}

func normalizeAddrs(addrs []string) []string {
	var a []string
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			a = append(a, addr)
		} else {
			a = append(a, fmt.Sprintf("raft://%s", addr))
			a = append(a, fmt.Sprintf("http://%s", addr))
			a = append(a, fmt.Sprintf("https://%s", addr))
		}
	}
	return a
}
