package http

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
)

const (
	minPortNumber = 0
	maxPortNumber = 65535
)

// ParseHostEnv parses a host URL from an environment variable and extracts its components.
// It returns the protocol scheme, hostname, port number, or any parsing error encountered.
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

	if uri.Port() == "" {
		return protocol, host, 0, nil
	}

	portStr := uri.Port()
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return "", "", 0, fmt.Errorf("port '%s' is not a valid number: %w", portStr, err)
	}

	if p < minPortNumber || p > maxPortNumber {
		return "", "", 0, fmt.Errorf("port number %d is outside of valid range [%d, %d]", p, minPortNumber, maxPortNumber)
	}
	return protocol, host, uint16(p), nil
}
