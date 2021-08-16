package tcp

import (
	"crypto/tls"
	"net"
	"time"
)

// NewDialer returns an initialized Dialer
func NewDialer(header byte, remoteEncrypted, skipVerify bool) *Dialer {
	return &Dialer{
		header:          header,
		remoteEncrypted: remoteEncrypted,
		skipVerify:      skipVerify,
	}
}

// Dialer supports dialing a cluster service.
type Dialer struct {
	header          byte
	remoteEncrypted bool
	skipVerify      bool
}

// Dial dials the cluster service at the given addr and returns a connection.
func (d *Dialer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if d.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: d.skipVerify,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}

	// Write a marker byte to indicate message type.
	_, err = conn.Write([]byte{d.header})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}
