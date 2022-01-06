package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	httpd "github.com/rqlite/rqlite/http"
)

var (
	// ErrUserInfoExists is returned when a join address already contains
	// a username and a password.
	ErrUserInfoExists = errors.New("userinfo exists")

	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")
)

// AddUserInfo adds username and password to the join address.
func AddUserInfo(joinAddr, username, password string) (string, error) {
	u, err := url.Parse(joinAddr)
	if err != nil {
		return "", err
	}

	if u.User != nil && u.User.Username() != "" {
		return "", ErrUserInfoExists
	}

	u.User = url.UserPassword(username, password)
	return u.String(), nil
}

// Join attempts to join the cluster at one of the addresses given in joinAddr.
// It walks through joinAddr in order, and sets the node ID and Raft address of
// the joining node as id addr respectively. It returns the endpoint successfully
// used to join the cluster.
func Join(srcIP string, joinAddr []string, id, addr string, voter bool, numAttempts int,
	attemptInterval time.Duration, tlsConfig *tls.Config) (string, error) {
	var err error
	var j string
	logger := log.New(os.Stderr, "[cluster-join] ", log.LstdFlags)
	if tlsConfig == nil {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	for i := 0; i < numAttempts; i++ {
		for _, a := range joinAddr {
			j, err = join(srcIP, a, id, addr, voter, tlsConfig, logger)
			if err == nil {
				// Success!
				return j, nil
			}
		}
		logger.Printf("failed to join cluster at %s: %s, sleeping %s before retry", joinAddr, err.Error(), attemptInterval)
		time.Sleep(attemptInterval)
	}
	logger.Printf("failed to join cluster at %s, after %d attempts", joinAddr, numAttempts)
	return "", ErrJoinFailed
}

func join(srcIP, joinAddr, id, addr string, voter bool, tlsConfig *tls.Config, logger *log.Logger) (string, error) {
	if id == "" {
		return "", fmt.Errorf("node ID not set")
	}
	// The specified source IP is optional
	var dialer *net.Dialer
	dialer = &net.Dialer{}
	if srcIP != "" {
		netAddr := &net.TCPAddr{
			IP:   net.ParseIP(srcIP),
			Port: 0,
		}
		dialer = &net.Dialer{LocalAddr: netAddr}
	}
	// Join using IP address, as that is what Hashicorp Raft works in.
	resv, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return "", err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := httpd.NormalizeAddr(fmt.Sprintf("%s/join", joinAddr))

	// Create and configure the client to connect to the other node.
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
		Dial:            dialer.Dial,
	}
	client := &http.Client{Transport: tr}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	for {
		b, err := json.Marshal(map[string]interface{}{
			"id":    id,
			"addr":  resv.String(),
			"voter": voter,
		})
		if err != nil {
			return "", err
		}

		// Attempt to join.
		resp, err := client.Post(fullAddr, "application/json", bytes.NewReader(b))
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			return fullAddr, nil
		case http.StatusMovedPermanently:
			fullAddr = resp.Header.Get("location")
			if fullAddr == "" {
				return "", fmt.Errorf("failed to join, invalid redirect received")
			}
			continue
		case http.StatusBadRequest:
			// One possible cause is that the target server is listening for HTTPS, but an HTTP
			// attempt was made. Switch the protocol to HTTPS, and try again. This can happen
			// when using the Disco service, since it doesn't record information about which
			// protocol a registered node is actually using.
			if strings.HasPrefix(fullAddr, "https://") {
				// It's already HTTPS, give up.
				return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
			}

			logger.Print("join via HTTP failed, trying via HTTPS")
			fullAddr = httpd.EnsureHTTPS(fullAddr)
			continue
		default:
			return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
		}
	}
}
