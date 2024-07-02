package rsync

import (
	"errors"
	"testing"
)

func Test_NewCAS(t *testing.T) {
	cas := NewCheckAndSet()
	if exp, got := false, cas.state; exp != got {
		t.Fatalf("expected %T, got %T", exp, got)
	}
}

func Test_CASBeginEnd(t *testing.T) {
	cas := NewCheckAndSet()
	if err := cas.Begin("foo"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if cas.Owner() != "foo" {
		t.Fatalf("expected foo, got %s", cas.Owner())
	}

	// Begin again, should fail
	if err := cas.Begin("bar"); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected %v, got %v", ErrCASConflict, err)
	}
	if cas.Owner() != "foo" {
		t.Fatalf("expected foo, got %s", cas.Owner())
	}

	// End, check owner, and another begin should succeed
	cas.End()
	if cas.Owner() != "" {
		t.Fatalf("expected empty string, got %s", cas.Owner())
	}
	if err := cas.Begin("qux"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
