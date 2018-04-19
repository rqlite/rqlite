package store

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func Test_NewTransport(t *testing.T) {
	if NewTransport() == nil {
		t.Fatal("failed to create new Transport")
	}
}

func Test_TransportOpenClose(t *testing.T) {
	tn := NewTransport()
	if err := tn.Open("localhost:0"); err != nil {
		t.Fatalf("failed to open transport: %s", err.Error())
	}
	if tn.Addr().String() == "localhost:0" {
		t.Fatal("transport address set incorrectly, got: %s", tn.Addr().String())
	}
	if err := tn.Close(); err != nil {
		t.Fatalf("failed to close transport: %s", err.Error())
	}
}

func Test_TransportDial(t *testing.T) {
	tn1 := NewTransport()
	tn1.Open("localhost:0")
	go tn1.Accept()
	tn2 := NewTransport()

	_, err := tn2.Dial(raft.ServerAddress(tn1.Addr().String()), time.Second)
	if err != nil {
		t.Fatalf("failed to connect to first transport: %s", err.Error())
	}
	tn1.Close()
}
