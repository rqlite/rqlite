package rsync

import (
	"fmt"
	"time"
)

// PollTrue is a utility for polling a function until it returns true or a timeout is reached.
type PollTrue struct {
	fn           func() bool
	pollInterval time.Duration
	timeout      time.Duration
}

// NewPollTrue creates a new PollTrue instance.
func NewPollTrue(fn func() bool, pollInterval time.Duration, timeout time.Duration) *PollTrue {
	return &PollTrue{
		fn:           fn,
		pollInterval: pollInterval,
		timeout:      timeout,
	}
}

// Run polls the function until it returns true or the timeout is reached.
func (rt *PollTrue) Run(name string) error {
	timer := time.NewTimer(rt.timeout)
	defer timer.Stop()
	ticker := time.NewTicker(rt.pollInterval)
	defer ticker.Stop()

	// Try fast path first
	if rt.fn() {
		return nil
	}

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout waiting for %s", name)
		case <-ticker.C:
			if rt.fn() {
				return nil
			}
		}
	}
}
