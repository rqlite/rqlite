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

type ConfigFunc func(*Client)

// Client is a wrapper around stock http.Client that adds "retry on another host" behaviour
// based on the supplied configuration.
//
// Errors that occur while performing requests are returned as they are, and the predicate to determine whether to check
// another host or not is the request returning an `http.StatusServiceUnavailable`.
// Note:
//
// This type is not goroutine safe.
type Client struct {
	*http.Client
	scheme string
	hosts  []string

	// creds stores the http basic authentication username and password
	creds  string
	logger *log.Logger

	// currentHost keeps track of the last available host
	currentHost int
	maxRedirect int
}

func NewClient(client *http.Client, hosts []string, configFuncs ...ConfigFunc) *Client {
	cl := &Client{
		Client:      client,
		hosts:       hosts,
		scheme:      "http",
		maxRedirect: 21,
		logger:      log.New(os.Stderr, "[client]", log.LstdFlags),
	}

	for _, f := range configFuncs {
		f(cl)
	}

	return cl
}

// WithScheme changes the default scheme used i.e "http"
func WithScheme(scheme string) ConfigFunc {
	return func(client *Client) {
		client.scheme = scheme
	}
}

// WithLogger changes the default logger to the one provided
func WithLogger(logger *log.Logger) ConfigFunc {
	return func(client *Client) {
		client.logger = logger
	}
}

// WithBasicAuth adds basic authentication behaviour to the client's request
func WithBasicAuth(creds string) ConfigFunc {
	return func(client *Client) {
		client.creds = creds
	}
}

func (c *Client) Query(url url.URL) (*http.Response, error) {
	return c.execRequest(http.MethodGet, url, nil)
}

func (c *Client) Execute(url url.URL, body io.Reader) (*http.Response, error) {
	return c.execRequest(http.MethodPost, url, body)
}

func (c *Client) execRequest(method string, url url.URL, body io.Reader) (*http.Response, error) {
	triedHosts := 0
	for triedHosts < len(c.hosts) {
		host := c.hosts[c.currentHost]
		url.Scheme = c.scheme
		url.Host = host
		urlStr := url.String()
		resp, err := c.requestFollowRedirect(method, urlStr, body)

		// If the status code is anything beside "service unavailable"
		// we should propagate the error back to the caller
		if resp != nil && resp.StatusCode != http.StatusServiceUnavailable {
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
