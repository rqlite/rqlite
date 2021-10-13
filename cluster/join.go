package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pastelnetwork/gonode/common/errors"
	"github.com/pastelnetwork/gonode/common/log"
	"github.com/pastelnetwork/gonode/common/utils"

	httpd "github.com/rqlite/rqlite/http"
)

var (
	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")
)

// Join attempts to join the cluster at one of the addresses given in joinAddr.
// It walks through joinAddr in order, and sets the node ID and Raft address of
// the joining node as id addr respectively. It returns the endpoint successfully
// used to join the cluster.
func Join(ctx context.Context, srcIP string, joinAddr []string, id, addr string, voter bool, numAttempts int,
	attemptInterval time.Duration, tlsConfig *tls.Config) (string, error) {
	var err error
	var j string

	if tlsConfig == nil {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	for i := 0; i < numAttempts; i++ {
		for _, a := range joinAddr {
			j, err = join(ctx, srcIP, a, id, addr, voter, tlsConfig)
			if err == nil {
				// Success!
				return j, nil
			}

			log.WithContext(ctx).Warnf("failed to join at %s: error: %s", a, err.Error())
		}

		log.WithContext(ctx).Warnf("failed to join cluster at %s: %s, sleeping %s before retry", joinAddr, utils.SafeErrStr(err), attemptInterval)
		time.Sleep(attemptInterval)
	}
	log.WithContext(ctx).Errorf("failed to join cluster at %s, after %d attempts", joinAddr, numAttempts)

	return "", ErrJoinFailed
}

func join(ctx context.Context, srcIP, joinAddr, id, addr string, voter bool, tlsConfig *tls.Config) (string, error) {
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
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		b, err := json.Marshal(map[string]interface{}{
			"id":    id,
			"addr":  resv.String(),
			"voter": voter,
		})
		if err != nil {
			return "", err
		}

		// Attempt to join.
		resp, err := client.Post(fullAddr, "application-type/json", bytes.NewReader(b))
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
			// One possible cause is that the target server is listening for HTTPS, but a HTTP
			// attempt was made. Switch the protocol to HTTPS, and try again. This can happen
			// when using the Disco service, since it doesn't record information about which
			// protocol a registered node is actually using.
			if strings.HasPrefix(fullAddr, "https://") {
				// It's already HTTPS, give up.
				return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
			}

			log.WithContext(ctx).Error("join via HTTP failed, trying via HTTPS")
			fullAddr = httpd.EnsureHTTPS(fullAddr)
			continue
		default:
			return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
		}
	}
}
