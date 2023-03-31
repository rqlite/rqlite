package url

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	// ErrUserInfoExists is returned when a join address already contains
	// a username and a password.
	ErrUserInfoExists = errors.New("userinfo exists")
)

// StringsToURLs converts a slice of strings to a slice of URLs. If the
// string does not contain a protocol, it is assumed to be HTTP.
func StringsToURLs(ss []string) ([]*url.URL, error) {
	urls := make([]*url.URL, 0, len(ss))
	for _, s := range ss {
		if !strings.Contains(s, "://") {
			s = "http://" + s
		}

		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		urls = append(urls, u)
	}
	return urls, nil
}

// NormalizeAddr ensures that the given URL has a HTTP protocol prefix.
// If none is supplied, it prefixes the URL with "http://".
func NormalizeAddr(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return fmt.Sprintf("http://%s", addr)
	}
	return addr
}

// EnsureHTTPS modifies the given URL, ensuring it is using the HTTPS protocol.
func EnsureHTTPS(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return fmt.Sprintf("https://%s", addr)
	}
	return strings.Replace(addr, "http://", "https://", 1)
}

// CheckHTTPS returns true if the given URL uses HTTPS.
func CheckHTTPS(addr string) bool {
	return strings.HasPrefix(addr, "https://")
}

// AddBasicAuth adds username and password to the join address. If username is empty
// joinAddr is returned unchanged. If joinAddr already contains a username, ErrUserInfoExists
// is returned.
func AddBasicAuth(joinAddr, username, password string) (string, error) {
	if username == "" {
		return joinAddr, nil
	}

	u, err := url.Parse(joinAddr)
	if err != nil {
		return "", err
	}

	if u.User != nil && u.User.Username() != "" {
		return "", ErrUserInfoExists
	}

	u.User = url.UserPassword(username, password)
	return u.String(), nil
}

// RemoveBasicAuth returns a copy of the given URL, with any basic auth password
// redacted.
func RemoveBasicAuth(u string) string {
	uu, err := url.Parse(u)
	if err != nil {
		return u
	}
	return uu.Redacted()
}
