package store

import (
	"errors"
	"sync/atomic"
)

var (
	// ErrCASConflict is returned when a CAS operation fails.
	ErrCASConflict = errors.New("cas conflict")
)

// CheckAndSet is a simple concurrency control mechanism that allows
// only one goroutine to execute a critical section at a time.
type CheckAndSet struct {
	state atomic.Int32
}

// NewCheckAndSet creates a new CheckAndSet instance.
func NewCheckAndSet() *CheckAndSet {
	return &CheckAndSet{}
}

// Begin attempts to enter the critical section. If another goroutine
// is already in the critical section, Begin returns an error.
func (c *CheckAndSet) Begin() error {
	if c.state.CompareAndSwap(0, 1) {
		return nil
	} else {
		return ErrCASConflict
	}
}

// End exits the critical section.
func (c *CheckAndSet) End() {
	c.state.Store(0)
}
