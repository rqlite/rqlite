package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	httpd "github.com/rqlite/rqlite/http"
)

const numAttempts int = 3
const attemptInterval time.Duration = 5 * time.Second

// It walks through joinAddr in order, and sets the node ID and Raft address of
// the joining node as nodeID advAddr respectively. It returns the endpoint
// successfully used to join the cluster.
func Join(joinAddr []string, nodeID, advAddr string, tlsConfig *tls.Config) (string, error) {
	var err error
	var j string
	logger := log.New(os.Stderr, "[cluster-join] ", log.LstdFlags)
	if tlsConfig == nil {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	for i := 0; i < numAttempts; i++ {
		for _, a := range joinAddr {
			j, err = join(a, nodeID, advAddr, tlsConfig, logger)
			if err == nil {
				// Success!
				return j, nil
			}
		}
		logger.Printf("failed to join cluster at %s: %s, sleeping %s before retry", joinAddr, err.Error(), attemptInterval)
		time.Sleep(attemptInterval)
	}
	logger.Printf("failed to join cluster at %s, after %d attempts", joinAddr, numAttempts)
	return "", err
}

func join(joinAddr, nodeID, advAddr string, tlsConfig *tls.Config, logger *log.Logger) (string, error) {
	if nodeID == "" {
		return "", fmt.Errorf("node ID not set")
	}

	// Join using IP address, as that is what Hashicorp Raft works in.
	resv, err := net.ResolveTCPAddr("tcp", advAddr)
	if err != nil {
		return "", err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := httpd.NormalizeAddr(fmt.Sprintf("%s/join", joinAddr))

	// Enable skipVerify as requested.
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := &http.Client{Transport: tr}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	for {
		b, err := json.Marshal(map[string]string{
			"id":   nodeID,
			"addr": resv.String(),
		})

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

			logger.Print("join via HTTP failed, trying via HTTPS")
			fullAddr = httpd.EnsureHTTPS(fullAddr)
			continue
		default:
			return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
		}
	}
}
