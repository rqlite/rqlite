package rsync

import (
	"sync"
	"testing"
	"time"
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

func Test_NewAtomicMap(t *testing.T) {
	m := NewAtomicMap[string, int]()
	if m == nil {
		t.Fatal("NewAtomicMap() returned nil")
	}

	// Test that new map is empty
	_, exists := m.Get("nonexistent")
	if exists {
		t.Fatal("Expected key not to exist in new map")
	}
}

func Test_AtomicMap_SetGet(t *testing.T) {
	m := NewAtomicMap[string, int]()

	// Test setting and getting a value
	m.Set("key1", 42)
	value, exists := m.Get("key1")
	if !exists {
		t.Fatal("Expected key1 to exist")
	}
	if value != 42 {
		t.Fatalf("Expected 42, got %d", value)
	}

	// Test getting non-existent key
	_, exists = m.Get("nonexistent")
	if exists {
		t.Fatal("Expected nonexistent key to not exist")
	}

	// Test overwriting a value
	m.Set("key1", 100)
	value, exists = m.Get("key1")
	if !exists {
		t.Fatal("Expected key1 to still exist after overwrite")
	}
	if value != 100 {
		t.Fatalf("Expected 100, got %d", value)
	}
}

func Test_AtomicMap_DifferentTypes(t *testing.T) {
	// Test with int keys and string values
	m1 := NewAtomicMap[int, string]()
	m1.Set(1, "one")
	m1.Set(2, "two")

	value, exists := m1.Get(1)
	if !exists || value != "one" {
		t.Fatalf("Expected 'one', got %q (exists: %v)", value, exists)
	}

	// Test with string keys and bool values
	m2 := NewAtomicMap[string, bool]()
	m2.Set("enabled", true)
	m2.Set("disabled", false)

	enabled, exists := m2.Get("enabled")
	if !exists || !enabled {
		t.Fatalf("Expected true, got %v (exists: %v)", enabled, exists)
	}

	disabled, exists := m2.Get("disabled")
	if !exists || disabled {
		t.Fatalf("Expected false, got %v (exists: %v)", disabled, exists)
	}
}

func Test_AtomicMap_ConcurrentAccess(t *testing.T) {
	m := NewAtomicMap[int, string]()

	const numGoroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup

	// Start multiple goroutines that write to the map
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := id*operationsPerGoroutine + j
				value := "value" + string(rune('0'+id)) + string(rune('0'+j%10))
				m.Set(key, value)
			}
		}(i)
	}

	// Start multiple goroutines that read from the map
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := id*operationsPerGoroutine + j
				// Give some time for writers to write
				time.Sleep(time.Microsecond)
				_, exists := m.Get(key)
				// We don't check the value since there might be a race,
				// but the operation should not panic or cause data races
				_ = exists
			}
		}(i)
	}

	wg.Wait()

	// Verify that all expected keys exist
	expectedKeys := numGoroutines * operationsPerGoroutine
	actualKeys := 0
	for i := 0; i < expectedKeys; i++ {
		if _, exists := m.Get(i); exists {
			actualKeys++
		}
	}

	if actualKeys != expectedKeys {
		t.Fatalf("Expected %d keys, got %d", expectedKeys, actualKeys)
	}
}
