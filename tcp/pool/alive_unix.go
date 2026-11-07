//go:build unix

package pool

import (
	"crypto/tls"
	"net"
	"syscall"
)

// alive reports whether an idle connection can still carry a request. An
// idle connection has nothing to read, so one which is readable -- because
// the remote end closed it, or sent anything at all -- is of no further use.
// The check does not block and sends nothing.
func alive(conn net.Conn) bool {
	if tc, ok := conn.(*tls.Conn); ok {
		conn = tc.NetConn()
	}
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return true
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return false
	}

	ok = false
	err = rc.Read(func(fd uintptr) bool {
		var b [1]byte
		for {
			_, err := syscall.Read(int(fd), b[:])
			if err == syscall.EINTR {
				continue
			}
			ok = err == syscall.EAGAIN || err == syscall.EWOULDBLOCK
			return true
		}
	})
	return err == nil && ok
}
