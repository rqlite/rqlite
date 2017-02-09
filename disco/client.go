// Package disco controls interaction with the rqlite Discovery service
package disco

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"runtime"
)

// Nodes represents a set of nodes currently registered at the configured Discovery URL.
type Nodes []string

// Client provides a Discovery Service client.
type Client struct {
	url string
}

// New returns an initialized Discovery Service client.
func New(url string) *Client {
	return &Client{
		url: url,
	}
}

// URL returns the Discovery Service URL used by this client.
func (c *Client) URL() string {
	return c.url
}

// Register attempts to register with the Discovery Service, using the given
// address.
func (c *Client) Register(addr string) (Nodes, error) {
	m := map[string]string{
		"addr":   addr,
		"GOOS":   runtime.GOOS,
		"GOARCH": runtime.GOARCH,
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(c.url, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}

	nodes := Nodes{}
	if err := json.Unmarshal(b, &nodes); err != nil {
		return nil, err
	}

	return nodes, nil
}
