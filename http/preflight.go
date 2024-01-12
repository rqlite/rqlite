package http

import (
	"crypto/tls"
	"net/http"
	"time"
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
	client := http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get("http://" + addr)
	if err == nil {
		resp.Body.Close()
		return true
	}

	// Check for a HTTPS server listening on the same address, using the same URL.
	// Don't check the certificate, as we're only interested in whether there's
	// a server running.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client = http.Client{
		Transport: tr,
		Timeout:   2 * time.Second,
	}
	resp, err = client.Get("https://" + addr + "/status")
	if err == nil {
		resp.Body.Close()
		return true
	}
	return false
}
