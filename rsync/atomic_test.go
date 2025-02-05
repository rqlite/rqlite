package rsync

import (
	"testing"
)

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

func Test_AtomicStringSlice(t *testing.T) {
	s := NewAtomicStringSlice()
	if s == nil {
		t.Errorf("NewAtomicStringSlice() returned nil")
	}
	if len(s.Load()) != 0 {
		t.Errorf("Expected 0, got %d", len(s.Load()))
	}
	if !s.Equals(nil) {
		t.Errorf("Expected true, got false")
	}
	if !s.Equals([]string{}) {
		t.Errorf("Expected true, got false")
	}

	s.Store([]string{"a", "b"})
	if len(s.Load()) != 2 {
		t.Errorf("Expected 2, got %d", len(s.Load()))
	}
	s.Store([]string{"a", "b", "c"})
	if len(s.Load()) != 3 {
		t.Errorf("Expected 3, got %d", len(s.Load()))
	}
	s.Store([]string{"a", "b"})
	if len(s.Load()) != 2 {
		t.Errorf("Expected 2, got %d", len(s.Load()))
	}

	if s.Equals(nil) {
		t.Errorf("Expected false, got true")
	}
	if !s.Equals([]string{"a", "b"}) {
		t.Errorf("Expected true, got false")
	}
	if s.Equals([]string{"a", "b", "c"}) {
		t.Errorf("Expected false, got true")
	}
}
