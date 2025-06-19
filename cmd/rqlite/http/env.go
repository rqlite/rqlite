package http

import (
	"net/url"
	"os"
	"strconv"
)

func ParseHostEnv() (protocol, host string, port uint16, succ bool) {
	succ = false

	hostEnv := os.Getenv("RQLITE_HOST")
	uri, err := url.Parse(hostEnv)
	if err != nil {
		return
	}

	if uri.Scheme == "" {
		protocol = "http"
	} else if uri.Scheme != "http" && uri.Scheme != "https" {
		return
	} else {
		protocol = uri.Scheme
	}

	if uri.Hostname() == "" {
		return
	}
	host = uri.Hostname()

	p, err := strconv.Atoi(uri.Port())
	if err != nil {
		// uri.Port() is empty
		port = 4001
	} else {
		port = uint16(p)
	}

	succ = true
	return
}
