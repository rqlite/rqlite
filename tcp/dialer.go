package tcp

import (
	"crypto/tls"
	"fmt"
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
func (d *Dialer) Dial(addr string, timeout time.Duration) (conn net.Conn, retErr error) {
	dialer := &net.Dialer{Timeout: timeout}

	if d.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: d.skipVerify,
		}
		conn, retErr = tls.DialWithDialer(dialer, "tcp", addr, conf)
	} else {
		conn, retErr = dialer.Dial("tcp", addr)
	}
	if retErr != nil {
		return nil, retErr
	}
	defer func() {
		if retErr != nil {
			conn.Close()
		}
	}()

	// Write a marker byte to indicate message type.
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("failed to set WriteDeadline for header: %s", err.Error())
	}
	if _, err := conn.Write([]byte{d.header}); err != nil {
		return nil, err
	}
	return conn, nil
}
