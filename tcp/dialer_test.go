package tcp

import (
	"net"
	"testing"
	"time"
)

func Test_NewDialer(t *testing.T) {
	d := NewDialer(1, false, false)
	if d == nil {
		t.Fatal("failed to create a dialer")
	}
}

func Test_DialerNoConnect(t *testing.T) {
	d := NewDialer(87, false, false)
	_, err := d.Dial("127.0.0.1:0", 5*time.Second)
	if err == nil {
		t.Fatalf("no error connecting to bad address")
	}
}

func Test_DialerHeader(t *testing.T) {
	s := mustNewEchoServer()
	defer s.Close()

	go s.MustStart()

	d := NewDialer(87, false, false)
	conn, err := d.Dial(s.Addr(), 10*time.Second)
	if err != nil {
		t.Fatalf("failed to dial echo server: %s", err.Error())
	}

	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from echo server: %s", err.Error())
	}
	if exp, got := buf[0], byte(87); exp != got {
		t.Fatalf("got wrong response from echo server, exp %d, got %d", exp, got)
	}
}

type echoSever struct {
	ln net.Listener
}

// Addr returns the address of the echo server.
func (e *echoSever) Addr() string {
	return e.ln.Addr().String()
}

// MustStart starts the echo server.
func (e *echoSever) MustStart() {
	for {
		conn, err := e.ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			buf := make([]byte, 1)
			_, err := c.Read(buf)
			if err != nil {
				panic("failed to read byte")
			}
			_, err = c.Write(buf)
			if err != nil {
				panic("failed to echo received byte")
			}
			c.Close()
		}(conn)
	}
}

// Close closes the echo server.
func (e *echoSever) Close() {
	e.ln.Close()
}

func mustNewEchoServer() *echoSever {
	return &echoSever{
		ln: mustTCPListener("127.0.0.1:0"),
	}
}
