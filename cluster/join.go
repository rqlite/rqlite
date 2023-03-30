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
	"os"
	"strings"
	"time"

	rurl "github.com/rqlite/rqlite/http/url"
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
)

// Joiner executes a node-join operation.
type Joiner struct {
	srcIP           string
	numAttempts     int
	attemptInterval time.Duration
	tlsConfig       *tls.Config

	username string
	password string

	client *http.Client

	logger *log.Logger
}

// NewJoiner returns an instantiated Joiner.
func NewJoiner(srcIP string, numAttempts int, attemptInterval time.Duration,
	tlsCfg *tls.Config) *Joiner {

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
		tlsConfig:       tlsCfg,
		logger:          log.New(os.Stderr, "[cluster-join] ", log.LstdFlags),
	}
	if joiner.tlsConfig == nil {
		joiner.tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Create and configure the client to connect to the other node.
	tr := &http.Transport{
		TLSClientConfig:   joiner.tlsConfig,
		Dial:              dialer.Dial,
		ForceAttemptHTTP2: true,
	}
	joiner.client = &http.Client{Transport: tr, Timeout: 10 * time.Second}
	joiner.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return joiner
}

// SetBasicAuth sets Basic Auth credentials for any join attempt.
func (j *Joiner) SetBasicAuth(username, password string) {
	j.username, j.password = username, password
}

// Do makes the actual join request.
func (j *Joiner) Do(joinAddrs []string, id, addr string, voter bool) (string, error) {
	if id == "" {
		return "", ErrNodeIDRequired
	}

	var err error
	var joinee string
	for i := 0; i < j.numAttempts; i++ {
		for _, a := range joinAddrs {
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
	// Check for protocol scheme, and insert default if necessary.
	fullAddr := rurl.NormalizeAddr(fmt.Sprintf("%s/join", joinAddr))

	for {
		b, err := json.Marshal(map[string]interface{}{
			"id":    id,
			"addr":  addr,
			"voter": voter,
		})
		if err != nil {
			return "", err
		}

		// Attempt to join.
		req, err := http.NewRequest("POST", fullAddr, bytes.NewReader(b))
		if err != nil {
			return "", err
		}
		if j.username != "" && j.password != "" {
			req.SetBasicAuth(j.username, j.password)
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := j.client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		b, err = io.ReadAll(resp.Body)
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
		case http.StatusBadRequest:
			// One possible cause is that the target server is listening for HTTPS, but an HTTP
			// attempt was made. Switch the protocol to HTTPS, and try again. This can happen
			// when using the Disco service, since it doesn't record information about which
			// protocol a registered node is actually using.
			if strings.HasPrefix(fullAddr, "https://") {
				// It's already HTTPS, give up.
				return "", fmt.Errorf("%s: (%s)", resp.Status, string(b))
			}

			j.logger.Print("join via HTTP failed, trying via HTTPS")
			fullAddr = rurl.EnsureHTTPS(fullAddr)
			continue
		default:
			return "", fmt.Errorf("%s: (%s)", resp.Status, string(b))
		}
	}
}
