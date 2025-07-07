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

func Test_NewAtomicBool_DefaultState(t *testing.T) {
	b := NewAtomicBool()
	if b.Is() {
		t.Fatal("expected default state false, got true")
	}
	if !b.IsNot() {
		t.Fatal("expected IsNot to be true for default state")
	}
}

func Test_Set_And_Unset(t *testing.T) {
	b := NewAtomicBool()

	b.Set()
	if !b.Is() {
		t.Fatal("after Set, expected true")
	}
	if b.IsNot() {
		t.Fatal("after Set, expected IsNot false")
	}

	b.Unset()
	if b.Is() {
		t.Fatal("after Unset, expected false")
	}
	if !b.IsNot() {
		t.Fatal("after Unset, expected IsNot true")
	}
}

func Test_SetBool_True(t *testing.T) {
	b := NewAtomicBool()
	b.SetBool(true)
	if !b.Is() {
		t.Fatal("after SetBool(true), expected true")
	}
	if b.IsNot() {
		t.Fatal("after SetBool(true), expected IsNot false")
	}
}

func Test_SetBool_False(t *testing.T) {
	b := NewAtomicBool()
	b.SetBool(false)
	if b.Is() {
		t.Fatal("after SetBool(false), expected false")
	}
	if !b.IsNot() {
		t.Fatal("after SetBool(false), expected IsNot true")
	}
}

func Test_Is_IsNot_Consistency(t *testing.T) {
	b := NewAtomicBool()

	// default false
	if got, want := b.Is(), false; got != want {
		t.Fatalf("Is() = %v, want %v", got, want)
	}
	if got, want := b.IsNot(), true; got != want {
		t.Fatalf("IsNot() = %v, want %v", got, want)
	}

	// flip to true
	b.Set()
	if got, want := b.Is(), true; got != want {
		t.Fatalf("Is() = %v, want %v", got, want)
	}
	if got, want := b.IsNot(), false; got != want {
		t.Fatalf("IsNot() = %v, want %v", got, want)
	}
}
