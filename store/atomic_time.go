package store

import (
	"sync"
	"time"
)

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
