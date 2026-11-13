package http

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	minPortNumber = 0
	maxPortNumber = 65535
)

// ParseHostEnv parses one or more comma-delimited host URLs from an environment
// variable. The components of the first URL are returned as the protocol scheme,
// hostname, and port number. Any remaining URLs are returned as a list of
// 'host:port' pairs, suitable for use as failover alternatives. Any URL which
// specifies a protocol must specify the same protocol as the first URL.
func ParseHostEnv(varName string) (protocol, host string, port uint16, alternatives []string, err error) {
	hostEnv, found := os.LookupEnv(varName)
	if !found {
		return "", "", 0, nil, fmt.Errorf("%s environment variable not set", varName)
	}

	var urls []string
	for _, u := range strings.Split(hostEnv, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		return "", "", 0, nil, fmt.Errorf("%s does not contain a hostname", varName)
	}

	protocol, host, port, _, err = parseHostURL(urls[0])
	if err != nil {
		return "", "", 0, nil, err
	}

	for _, u := range urls[1:] {
		p, _, _, hostPort, err := parseHostURL(u)
		if err != nil {
			return "", "", 0, nil, err
		}
		if p != "" && p != protocol {
			return "", "", 0, nil, fmt.Errorf("all URLs in %s must use the same protocol", varName)
		}
		alternatives = append(alternatives, hostPort)
	}
	return protocol, host, port, alternatives, nil
}

// ParseCredentialsEnv reads basic auth credentials from the environment.
// credsVar names a variable containing credentials in the form
// 'username:password'. If it is not set, the credentials are formed from the
// variables named by userVar and passwordVar. ok is false if neither credsVar
// nor userVar is set.
func ParseCredentialsEnv(credsVar, userVar, passwordVar string) (creds string, ok bool) {
	if creds, ok = os.LookupEnv(credsVar); ok {
		return creds, true
	}
	user, ok := os.LookupEnv(userVar)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%s:%s", user, os.Getenv(passwordVar)), true
}

// ParseBoolEnv parses a boolean value from an environment variable. ok is
// false if the variable is not set. Values accepted are those accepted by
// strconv.ParseBool.
func ParseBoolEnv(varName string) (value, ok bool, err error) {
	v, found := os.LookupEnv(varName)
	if !found {
		return false, false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, false, fmt.Errorf("%s must be a boolean value: %w", varName, err)
	}
	return b, true, nil
}

// parseHostURL parses a single host URL, returning the protocol scheme,
// hostname, and port number, along with the 'host:port' form of the URL, or
// any parsing error encountered.
func parseHostURL(hostURL string) (protocol, host string, port uint16, hostPort string, err error) {
	uri, err := url.Parse(hostURL)
	if err != nil {
		return "", "", 0, "", err
	}

	if uri.Scheme != "http" && uri.Scheme != "https" && uri.Scheme != "" {
		return "", "", 0, "", fmt.Errorf("unsupported protocol %s", uri.Scheme)
	}
	protocol = uri.Scheme

	if uri.Hostname() == "" {
		return "", "", 0, "", fmt.Errorf("'%s' does not contain a hostname", hostURL)
	}
	host = uri.Hostname()

	if uri.Port() == "" {
		return protocol, host, 0, uri.Host, nil
	}

	portStr := uri.Port()
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return "", "", 0, "", fmt.Errorf("port '%s' is not a valid number: %w", portStr, err)
	}

	if p < minPortNumber || p > maxPortNumber {
		return "", "", 0, "", fmt.Errorf("port number %d is outside of valid range [%d, %d]", p, minPortNumber, maxPortNumber)
	}
	return protocol, host, uint16(p), uri.Host, nil
}
