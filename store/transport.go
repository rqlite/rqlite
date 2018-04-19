package store

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// Transport is the network service provided to Raft
type Transport struct {
	ln net.Listener

	remoteEncrypted bool
	skipVerify      bool
}

func NewTransport() *Transport {
	return &Transport{}
}

func (t *Transport) Open(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.ln = ln
	return nil
}

func (t *Transport) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var err error
	var conn net.Conn
	if t.remoteEncrypted {
		conf := &tls.Config{
			InsecureSkipVerify: t.skipVerify,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", string(addr), conf)
	} else {
		conn, err = dialer.Dial("tcp", string(addr))
	}

	return conn, err
}

func (t *Transport) Accept() (net.Conn, error) {
	return t.ln.Accept()
}

func (t *Transport) Addr() net.Addr {
	return t.ln.Addr()
}

func (t *Transport) Close() error {
	return t.ln.Close()
}
