//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package pool

import (
	"crypto/tls"
	"net"
	"syscall"
)

// connClosed checks for a definite peer closure without consuming bytes or
// changing deadlines. A peer can still close immediately after this check.
func connClosed(conn net.Conn) bool {
	if tc, ok := conn.(*tls.Conn); ok {
		// Do not probe through TLS Read: a timeout can affect TLS state, and
		// encrypted control records may be pending even on an idle connection.
		conn = tc.NetConn()
	}
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	closed := false
	err = raw.Control(func(fd uintptr) {
		var b [1]byte
		n, _, err := syscall.Recvfrom(int(fd), b[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		closed = (err == nil && n == 0) || err == syscall.ECONNRESET || err == syscall.ENOTCONN
	})
	return err == nil && closed
}
