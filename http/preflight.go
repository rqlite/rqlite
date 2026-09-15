package http

import (
	"crypto/tls"
	"net/http"
	"time"
)

const (
	isServingTestPath = "/status"
	isServingTimeout  = 2 * time.Second
)

// AnyServingHTTP returns the first address in the list that appears to be
// serving HTTP or HTTPS, or false if none of them are.
func AnyServingHTTP(addrs []string) (string, bool) {
	for _, addr := range addrs {
		if IsServingHTTP(addr) {
			return addr, true
		}
	}
	return "", false
}

// IsServingHTTP returns true if there appears to be a HTTP or HTTPS server
// running on the given address.
func IsServingHTTP(addr string) bool {
	urlStr := addr + isServingTestPath

	// Set up the HTTP(S) client, ignoring certificate errors since we're only
	// interested in whether there's a server running.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := http.Client{
		Transport: tr,
		Timeout:   isServingTimeout,
	}

	for _, u := range []string{
		"http://" + urlStr,
		"https://" + urlStr,
	} {
		resp, err := client.Get(u)
		if err == nil {
			resp.Body.Close()
			return true
		}
	}
	return false
}
