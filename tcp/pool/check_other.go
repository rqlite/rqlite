//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package pool

import "net"

// connClosed conservatively retains connections where socket peeking is unavailable.
func connClosed(net.Conn) bool { return false }
