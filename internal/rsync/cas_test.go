package rsync

import (
	"errors"
	"testing"
	"time"
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

	// Retrying should also fail.
	if err := cas.BeginWithRetry("bar", 100*time.Millisecond, 500*time.Millisecond); !errors.Is(err, ErrCASConflictTimeout) {
		t.Fatalf("expected %v, got %v", ErrCASConflict, err)
	}

	// End, check owner, and another begin should succeed
	cas.End()
	if cas.Owner() != "" {
		t.Fatalf("expected empty string, got %s", cas.Owner())
	}
	if err := cas.Begin("qux"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// Unlock and check retry.
	cas.End()
	if err := cas.BeginWithRetry("bar", 100*time.Millisecond, 500*time.Millisecond); err != nil {
		t.Fatalf("expected %v, got %v", ErrCASConflict, err)
	}
}
