package http

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// ErrNoAvailableHost indicates that the client could not find an available host to send the request to.
var ErrNoAvailableHost = fmt.Errorf("no host available to perform the request")

// ErrTooManyRedirects indicates that the client exceeded the maximum number of redirects
var ErrTooManyRedirects = fmt.Errorf("maximum leader redirect limit exceeded")

// ErrProtocolNotSpecified is returned when an invalid host is passed to the client
var ErrProtocolNotSpecified = fmt.Errorf("host must start with http:// or https://")

// HostChangedError indicates that the underlying request was executed on a different host
// different from the caller anticipated
type HostChangedError struct {
	NewHost string
}

func (he *HostChangedError) Error() string {
	return fmt.Sprintf("HostChangedErr: new host is '%s'", he.NewHost)
}

type ConfigFunc func(*Client)

// Client is a wrapper around stock `http.Client` that adds "retry on another host" behaviour
// based on the supplied configuration.
//
// The client will fall back and try other nodes when the current node is unavailable, and would stop trying
// after exhausting the list of supplied hosts.
//
// Note:
//
// This type is not goroutine safe.
// A node is considered unavailable if the client is not reachable via the network.
// TODO: make the unavailability condition for the client more dynamic.
type Client struct {
	*http.Client
	hosts  []*url.URL
	Prefix string

	// creds stores the http basic authentication username and password
	creds  string
	logger *log.Logger

	// currentHost keeps track of the last available host
	currentHost int
	maxRedirect int
}

// NewClient creates a default client that sends `execute` and query `requests` against the
// rqlited nodes supplied via `hosts` argument.
func NewClient(client *http.Client, hosts []string, configFuncs ...ConfigFunc) (*Client, error) {
	var err error
	uHosts := make([]*url.URL, len(hosts))
	for i := range hosts {
		if !strings.HasPrefix(hosts[i], "http://") && !strings.HasPrefix(hosts[i], "https://") {
			return nil, ErrProtocolNotSpecified
		}
		uHosts[i], err = url.Parse(hosts[i])
		if err != nil {
			return nil, err
		}
	}

	cl := &Client{
		Client:      client,
		hosts:       uHosts,
		maxRedirect: 21,
		Prefix:      "/",
		logger:      log.New(os.Stderr, "[client] ", log.LstdFlags),
	}

	for _, f := range configFuncs {
		f(cl)
	}

	return cl, nil
}

// WithPrefix sets the prefix to be used when issuing HTTP requests against one of
// the rqlited nodes.
func WithPrefix(prefix string) ConfigFunc {
	return func(client *Client) {
		client.Prefix = prefix
	}
}

// WithLogger changes the default logger to the one provided.
func WithLogger(logger *log.Logger) ConfigFunc {
	return func(client *Client) {
		client.logger = logger
	}
}

// WithBasicAuth adds basic authentication behaviour to the client's request.
func WithBasicAuth(creds string) ConfigFunc {
	return func(client *Client) {
		client.creds = creds
	}
}

// Query sends GET requests to one of the hosts known to the client.
func (c *Client) Query(url url.URL) (*http.Response, error) {
	return c.execRequest(http.MethodGet, url, nil)
}

// Execute sends POST requests to one of the hosts known to the client
func (c *Client) Execute(url url.URL, body io.Reader) (*http.Response, error) {
	return c.execRequest(http.MethodPost, url, body)
}

func (c *Client) execRequest(method string, relativeURL url.URL, body io.Reader) (*http.Response, error) {
	triedHosts := 0
	for triedHosts < len(c.hosts) {
		host := c.hosts[c.currentHost]

		// Get protocol and host for current node, and merge with relative URL passed in.
		relativeURL.Scheme = host.Scheme
		relativeURL.Host = host.Host
		urlStr := relativeURL.String()
		resp, err := c.requestFollowRedirect(method, urlStr, body)

		// Found a responsive node
		if err == nil {
			if triedHosts > 0 {
				return resp, &HostChangedError{NewHost: host.String()}
			}
			return resp, nil
		}

		// If we did too many redirects, we will consider the host as unavailable as well,
		// and we will retry the request from another host
		if err == ErrTooManyRedirects {
			c.logger.Printf("too many redirects from host: '%s'", host)
		}

		c.logger.Printf("host '%s' is unavailable, retrying with the next available host", host)
		triedHosts++
		c.nextHost()
	}

	c.logger.Printf("none of the available hosts are responsive")
	return nil, ErrNoAvailableHost
}

func (c *Client) nextHost() {
	c.currentHost = (c.currentHost + 1) % len(c.hosts)
}

func (c *Client) requestFollowRedirect(method string, urlStr string, body io.Reader) (*http.Response, error) {
	nRedirects := 0
	for {
		req, err := http.NewRequest(method, urlStr, body)
		if err != nil {
			return nil, err
		}
		err = c.setBasicAuth(req)
		if err != nil {
			return nil, err
		}

		resp, err := c.Client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusMovedPermanently {
			nRedirects++
			if nRedirects > c.maxRedirect {
				return resp, ErrTooManyRedirects
			}
			urlStr = resp.Header["Location"][0]
			continue
		}

		return resp, nil
	}
}

func (c *Client) setBasicAuth(req *http.Request) error {
	if c.creds == "" {
		return nil
	}
	creds := strings.Split(c.creds, ":")
	if len(creds) != 2 {
		return fmt.Errorf("invalid Basic Auth credential format")
	}
	req.SetBasicAuth(creds[0], creds[1])
	return nil
}
