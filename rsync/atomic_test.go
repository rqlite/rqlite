package rsync

import "testing"

func Test_AtomicMonotonicUin64(t *testing.T) {
	u := NewAtomicMonotonicUint64()
	if u == nil {
		t.Errorf("NewAtomicMonotonicUint64() returned nil")
	}
	if u.Load() != 0 {
		t.Errorf("Expected 0, got %d", u.Load())
	}
	u.Store(1)
	if u.Load() != 1 {
		t.Errorf("Expected 1, got %d", u.Load())
	}
	u.Store(0)
	if u.Load() != 1 {
		t.Errorf("Expected 1, got %d", u.Load())
	}
	u.Store(2)
	if u.Load() != 2 {
		t.Errorf("Expected 2, got %d", u.Load())
	}
	u.Reset()
	if u.Load() != 0 {
		t.Errorf("Expected 0, got %d", u.Load())
	}
	u.Store(1)
	if u.Load() != 1 {
		t.Errorf("Expected 1, got %d", u.Load())
	}

}
