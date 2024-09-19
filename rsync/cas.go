package rsync

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrCASConflict is returned when a CAS operation fails.
	ErrCASConflict = errors.New("CAS conflict")

	// ErrCASConflictTimeout is returned when a CAS operation fails
	// even after retrying.
	ErrCASConflictTimeout = errors.New("CAS conflict timeout")
)

// CheckAndSet is a simple concurrency control mechanism that allows
// only one goroutine to execute a critical section at a time.
type CheckAndSet struct {
	state  bool
	owner  string
	startT time.Time
	mu     sync.Mutex
}

// NewCheckAndSet creates a new CheckAndSet instance.
func NewCheckAndSet() *CheckAndSet {
	return &CheckAndSet{}
}

// Begin attempts to enter the critical section. If another goroutine
// is already in the critical section, Begin returns an error of type
// ErrCASConflict.
func (c *CheckAndSet) Begin(owner string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state {
		return fmt.Errorf(`%w: currently held by owner "%s" for %s`, ErrCASConflict, c.owner, time.Since(c.startT))
	}
	c.owner = owner
	c.state = true
	c.startT = time.Now()
	return nil
}

// BeginWithRetry will attempt to enter the critical section, retrying
// if necessary.
func (c *CheckAndSet) BeginWithRetry(owner string, timeout, retryInterval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := c.Begin(owner)
		if err == nil {
			// Successfully acquired the lock
			return nil
		}

		// If the error is not a CAS conflict, return it
		if !errors.Is(err, ErrCASConflict) {
			return err
		}

		// Check if timeout has been reached
		if time.Now().After(deadline) {
			return ErrCASConflictTimeout
		}

		// Sleep for the retry interval before trying again
		time.Sleep(retryInterval)
	}
}

// End exits the critical section.
func (c *CheckAndSet) End() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owner = ""
	c.state = false
	c.startT = time.Time{}
}

// Owner returns the current owner of the critical section.
func (c *CheckAndSet) Owner() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owner
}

// Stats returns diagnostic information about the current state of the
// CheckAndSet instance.
func (c *CheckAndSet) Stats() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := map[string]interface{}{
		"owner": nil,
	}
	if c.state {
		stats["owner"] = c.owner
		stats["duration"] = time.Since(c.startT)
	}
	return stats
}
