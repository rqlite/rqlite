package tcp

import (
	"os"
	"testing"
	"time"

	"github.com/rqlite/rqlite/testdata/x509"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport() == nil {
		t.Fatal("failed to create new Transport")
	}
}

func Test_TransportOpenClose(t *testing.T) {
	t.Parallel()

	tn := NewTransport()
	if err := tn.Open("localhost:0"); err != nil {
		t.Fatalf("failed to open transport: %s", err.Error())
	}
	if tn.Addr().String() == "localhost:0" {
		t.Fatalf("transport address set incorrectly, got: %s", tn.Addr().String())
	}
	if err := tn.Close(); err != nil {
		t.Fatalf("failed to close transport: %s", err.Error())
	}
}

func Test_TransportDial(t *testing.T) {
	t.Parallel()

	tn1 := NewTransport()
	tn1.Open("localhost:0")
	go tn1.Accept()
	tn2 := NewTransport()

	_, err := tn2.Dial(tn1.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect to first transport: %s", err.Error())
	}
	tn1.Close()
}

func Test_NewTLSTransport(t *testing.T) {
	t.Parallel()

	c := x509.CertFile()
	defer os.Remove(c)
	k := x509.KeyFile()
	defer os.Remove(k)

	if NewTLSTransport(c, k, true) == nil {
		t.Fatal("failed to create new TLS Transport")
	}
}

func Test_TLSTransportOpenClose(t *testing.T) {
	t.Parallel()

	c := x509.CertFile()
	defer os.Remove(c)
	k := x509.KeyFile()
	defer os.Remove(k)

	tn := NewTLSTransport(c, k, true)
	if err := tn.Open("localhost:0"); err != nil {
		t.Fatalf("failed to open TLS transport: %s", err.Error())
	}
	if tn.Addr().String() == "localhost:0" {
		t.Fatalf("TLS transport address set incorrectly, got: %s", tn.Addr().String())
	}
	if err := tn.Close(); err != nil {
		t.Fatalf("failed to close TLS transport: %s", err.Error())
	}
}
