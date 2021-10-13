// Package disco controls interaction with the rqlite Discovery service
package disco

import (
	"bytes"
	"context"
	"encoding/json"

	"fmt"
	"io/ioutil"

	"net/http"

	"github.com/pastelnetwork/gonode/common/errors"
	"github.com/pastelnetwork/gonode/common/log"
)

// Response represents the response returned by a Discovery Service.
type Response struct {
	CreatedAt string   `json:"created_at"`
	DiscoID   string   `json:"disco_id"`
	Nodes     []string `json:"nodes"`
}

// Client provides a Discovery Service client.
type Client struct {
	url string
	ctx context.Context
}

// New returns an initialized Discovery Service client.
func New(ctx context.Context, url string) *Client {
	return &Client{
		url: url,
		ctx: ctx,
	}
}

// URL returns the Discovery Service URL used by this client.
func (c *Client) URL() string {
	return c.url
}

// Register attempts to register with the Discovery Service, using the given
// address.
func (c *Client) Register(id, addr string) (*Response, error) {
	m := map[string]string{
		"addr": addr,
	}

	url := c.registrationURL(c.url, id)
	client := &http.Client{}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	for {
		b, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}

		log.WithContext(c.ctx).Infof("discovery client attempting registration of %s at %s", addr, url)
		resp, err := client.Post(url, "application-type/json", bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			r := &Response{}
			if err := json.Unmarshal(b, r); err != nil {
				return nil, err
			}
			log.WithContext(c.ctx).Infof("discovery client successfully registered %s at %s", addr, url)
			return r, nil
		case http.StatusMovedPermanently:
			url = resp.Header.Get("location")
			log.WithContext(c.ctx).Infof("discovery client redirecting to: %s", url)
			continue
		default:
			return nil, errors.New(resp.Status)
		}
	}
}

func (c *Client) registrationURL(url, id string) string {
	return fmt.Sprintf("%s/%s", url, id)
}
