package rsync

import (
	"sync"
	"sync/atomic"
	"time"
)

// AtomicMonotonicUint64 is a uint64 with atomic operations which will
// only store the maximum value.
type AtomicMonotonicUint64 struct {
	value uint64
	mu    sync.Mutex
}

// NewAtomicMonotonicUint64 returns a new AtomicMonotonicUint64.
func NewAtomicMonotonicUint64() *AtomicMonotonicUint64 {
	return &AtomicMonotonicUint64{}
}

// Load returns the stored value.
func (a *AtomicMonotonicUint64) Load() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.value
}

// Store stores a new value if it is greater than the current value.
func (a *AtomicMonotonicUint64) Store(v uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if v > a.value {
		a.value = v
	}
}

// Reset resets the stored value to 0.
func (a *AtomicMonotonicUint64) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.value = 0
}

// AtomicTime is a time.Time with atomic operations.
type AtomicTime struct {
	t  time.Time
	mu sync.RWMutex
}

// NewAtomicTime returns a new AtomicTime.
func NewAtomicTime() *AtomicTime {
	return &AtomicTime{}
}

// Store stores a new time.
func (t *AtomicTime) Store(newTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.t = newTime
}

// Load returns the stored time.
func (t *AtomicTime) Load() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.t
}

// Add adds a duration to the stored time.
func (t *AtomicTime) Add(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.t = t.t.Add(d)
}

// Sub return the difference between the stored time and the given time.
func (t *AtomicTime) Sub(tt *AtomicTime) time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.t.Sub(tt.t)
}

// IsZero returns true if the stored time is zero, false otherwise.
func (t *AtomicTime) IsZero() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.t.IsZero()
}

// AtomicBool is a boolean with atomic operations.
type AtomicBool struct {
	state int32 // 1 for true, 0 for false
}

// NewAtomicBool returns a new AtomicBool initialized to false.
func NewAtomicBool() *AtomicBool {
	return &AtomicBool{state: 0}
}

// Set sets the AtomicBool to true.
func (b *AtomicBool) Set() {
	atomic.StoreInt32(&b.state, 1)
}

// Unset sets the AtomicBool to false.
func (b *AtomicBool) Unset() {
	atomic.StoreInt32(&b.state, 0)
}

// Is returns true if the AtomicBool is true, false otherwise.
func (b *AtomicBool) Is() bool {
	return atomic.LoadInt32(&b.state) == 1
}

// AtomicString is a string with atomic operations.
type AtomicString struct {
	s  string
	mu sync.RWMutex
}

// NewAtomicString returns a new AtomicString.
func NewAtomicString() *AtomicString {
	return &AtomicString{}
}

// Store stores a new string.
func (s *AtomicString) Store(newString string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.s = newString
}

// Load returns the stored string.
func (s *AtomicString) Load() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s
}
