package tcp

import (
	"testing"
)

func Test_NameAddress(t *testing.T) {
	n := NameAddress{"feynman:1234"}
	if n.Network() != "tcp" {
		t.Fatalf("wrong network returned")
	}
	if n.String() != "feynman:1234" {
		t.Fatalf("wrong address returned")
	}
}
