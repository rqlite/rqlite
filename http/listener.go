package http

import "net"

// DefaultListener returns a TCP listener bound to addr, suitable for
// passing to New.
func DefaultListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
