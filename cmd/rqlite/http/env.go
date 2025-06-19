package http

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
)

func ParseHostEnv() (protocol, host string, port uint16, err error) {
	hostEnv, found := os.LookupEnv("RQLITE_HOST")
	if !found {
		return "", "", 0, fmt.Errorf("RQLITE_HOST environment variable not set")
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
		return "", "", 0, fmt.Errorf("RQLITE_HOST does not contain a hostname")
	}
	host = uri.Hostname()

	p, err := strconv.Atoi(uri.Port())
	if err != nil {
		// uri.Port() is empty
		return protocol, host, 0, nil
	}

	return protocol, host, uint16(p), nil
}
