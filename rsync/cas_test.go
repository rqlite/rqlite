package rsync

import "testing"

func Test_NewCAS(t *testing.T) {
	cas := NewCheckAndSet()
	if exp, got := int32(0), cas.state.Load(); exp != got {
		t.Fatalf("expected %d, got %d", exp, got)
	}
}

func Test_CASBeginEnd(t *testing.T) {
	cas := NewCheckAndSet()
	if err := cas.Begin(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// Begin again, should fail
	if err := cas.Begin(); err != ErrCASConflict {
		t.Fatalf("expected %v, got %v", ErrCASConflict, err)
	}

	// End, another begin should succeed
	cas.End()
	if err := cas.Begin(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
