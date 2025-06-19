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
		err = fmt.Errorf("RQLITE_HOST environment variable not set")
		return
	}

	uri, err := url.Parse(hostEnv)
	if err != nil {
		return
	}

	if uri.Scheme == "" {
		protocol = "http"
	} else if uri.Scheme != "http" && uri.Scheme != "https" {
		err = fmt.Errorf("unsupported protocol %s", uri.Scheme)
		return
	} else {
		protocol = uri.Scheme
	}

	if uri.Hostname() == "" {
		err = fmt.Errorf("RQLITE_HOST does not contain a hostname")
		return
	}
	host = uri.Hostname()

	p, err := strconv.Atoi(uri.Port())
	if err != nil {
		// uri.Port() is empty
		err = nil
		port = 4001
	} else {
		port = uint16(p)
	}

	return
}
