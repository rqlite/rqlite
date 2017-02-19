package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	httpd "github.com/rqlite/rqlite/http"
	"io/ioutil"
	"net"
	"net/http"
)

// Join attempts to join the cluster at one of the addresses given in joinAddr.
// It walks through joinAddr in order, and sets the Raft address of the joining
// node as advAddr. It returns the endpoint successfully used to join the cluster.
func Join(joinAddr []string, advAddr string, skipVerify bool) (string, error) {
	var err error
	var j string
	for _, a := range joinAddr {
		j, err = join(a, advAddr, skipVerify)
		if err == nil {
			// Success!
			return j, nil
		}
	}
	return "", err
}

func join(joinAddr string, advAddr string, skipVerify bool) (string, error) {
	// Join using IP address, as that is what Hashicorp Raft works in.
	resv, err := net.ResolveTCPAddr("tcp", advAddr)
	if err != nil {
		return "", err
	}

	// Check for protocol scheme, and insert default if necessary.
	fullAddr := httpd.NormalizeAddr(fmt.Sprintf("%s/join", joinAddr))

	// Enable skipVerify as requested.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: skipVerify},
	}
	client := &http.Client{Transport: tr}

	for {
		b, err := json.Marshal(map[string]string{"addr": resv.String()})
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
		default:
			return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
		}
	}
}
