package http

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
)

// ParseHostEnv parses a host URL from an environment variable and extracts its components.
// The URL must use http, https, or no protocol scheme
// The URL must contain a hostname.
// If no port is specified in the URL, the port return value will be 0.
// Returns the protocol scheme, hostname, port number, and any parsing error encountered.
func ParseHostEnv(varName string) (protocol, host string, port uint16, err error) {
	hostEnv, found := os.LookupEnv(varName)
	if !found {
		return "", "", 0, fmt.Errorf("%s environment variable not set", varName)
	}

	uri, err := url.Parse(hostEnv)
	if err != nil {
		return "", "", 0, err
	}

	if uri.Scheme != "http" && uri.Scheme != "https" && uri.Scheme != "" {
		return "", "", 0, fmt.Errorf("unsupported protocol %s", uri.Scheme)
	}
	protocol = uri.Scheme

	if uri.Hostname() == "" {
		return "", "", 0, fmt.Errorf("%s does not contain a hostname", varName)
	}
	host = uri.Hostname()

	p, err := strconv.Atoi(uri.Port())
	if err != nil {
		// uri.Port() is empty
		return protocol, host, 0, nil
	}

	return protocol, host, uint16(p), nil
}
