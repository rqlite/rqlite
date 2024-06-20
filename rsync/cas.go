package rsync

import (
	"errors"
	"sync/atomic"
)

var (
	// ErrCASConflict is returned when a CAS operation fails.
	ErrCASConflict = errors.New("CAS conflict")
)

// CheckAndSet is a simple concurrency control mechanism that allows
// only one goroutine to execute a critical section at a time.
type CheckAndSet struct {
	state atomic.Int32
	owner AtomicString
}

// NewCheckAndSet creates a new CheckAndSet instance.
func NewCheckAndSet() *CheckAndSet {
	return &CheckAndSet{}
}

// Begin attempts to enter the critical section. If another goroutine
// is already in the critical section, Begin returns an error.
func (c *CheckAndSet) Begin(owner string) error {
	if c.state.CompareAndSwap(0, 1) {
		c.owner.Store(owner)
		return nil
	}
	return ErrCASConflict
}

// End exits the critical section.
func (c *CheckAndSet) End() {
	c.owner.Store("")
	c.state.Store(0)
}

// Owner returns the current owner of the critical section.
func (c *CheckAndSet) Owner() string {
	return c.owner.Load()
}

// Stats returns diagnostic information about the current state of the
// CheckAndSet instance.
func (c *CheckAndSet) Stats() map[string]interface{} {
	return map[string]interface{}{
		"owner": c.owner.Load(),
	}
}
