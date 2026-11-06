//go:build !unix

package pool

import "net"

// alive reports whether an idle connection can still carry a request. It
// is not implemented on this platform, so every connection is presumed good.
func alive(conn net.Conn) bool {
	return true
}
