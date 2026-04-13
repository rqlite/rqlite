package throttler

import (
	"context"
	"sync"
	"time"
)

// Throttler gates write requests based on system load signals. When the
// system falls behind (e.g. snapshot reaping takes too long), callers
// invoke Signal to increase the delay applied to subsequent writes. When
// the system recovers, callers invoke Release to decrease the delay by
// releaseRate steps, allowing throughput to ramp up gradually.
//
// If no signal (Signal, Release, or Reset) is received within the
// configured idle timeout, the Throttler resets itself to zero delay.
type Throttler struct {
	mu          sync.RWMutex
	delayFactor int
	delays      []time.Duration
	releaseRate int

	idleTimeout time.Duration
	timer       *time.Timer
}

// New returns a Throttler with the given delay steps, release
// rate, and idle timeout. The first element of delays should be 0 (no
// delay). Each subsequent element represents the delay applied at that
// level of backpressure. The releaseRate controls how many levels are
// shed per Release call. If idleTimeout is positive, the Throttler
// resets to zero delay after that duration of inactivity. Pass 0 to
// disable the idle timeout.
func New(delays []time.Duration, releaseRate int, idleTimeout time.Duration) *Throttler {
	if len(delays) == 0 {
		delays = []time.Duration{0}
	}
	if releaseRate < 1 {
		releaseRate = 1
	}
	t := &Throttler{
		delays:      delays,
		releaseRate: releaseRate,
		idleTimeout: idleTimeout,
	}
	if idleTimeout > 0 {
		t.timer = time.AfterFunc(idleTimeout, t.Reset)
		t.timer.Stop() // don't start until first signal
	}
	return t
}

// DefaultThrottler returns a Throttler with a default set of delay steps,
// a release rate of 3, and a 30-second idle timeout.
func DefaultThrottler() *Throttler {
	return New([]time.Duration{
		0,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
	}, 3, 30*time.Second)
}

// touch restarts the idle timer. Must be called with t.mu held.
func (t *Throttler) touch() {
	if t.timer != nil {
		t.timer.Reset(t.idleTimeout)
	}
}

// Signal indicates that the system is under pressure. Each call increments
// the delay factor by one, up to the maximum level.
func (t *Throttler) Signal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.delayFactor < len(t.delays)-1 {
		t.delayFactor++
	}
	t.touch()
}

// Release indicates that the system has recovered one step. Each call
// decrements the delay factor by releaseRate, down to zero. This gradual
// decay avoids oscillation when load is near the system's capacity.
func (t *Throttler) Release() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delayFactor -= t.releaseRate
	if t.delayFactor < 0 {
		t.delayFactor = 0
	}
	t.touch()
}

// Reset removes all delay immediately.
func (t *Throttler) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delayFactor = 0
	if t.timer != nil {
		t.timer.Stop()
	}
}

// Delay blocks for the duration corresponding to the current delay factor.
// It respects context cancellation so clients can time out. Returns nil if
// the delay completed, or the context error if the caller gave up.
func (t *Throttler) Delay(ctx context.Context) error {
	t.mu.RLock()
	d := t.delays[t.delayFactor]
	t.mu.RUnlock()
	if d == 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetDelay returns the current delay duration without blocking.
func (t *Throttler) GetDelay() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.delays[t.delayFactor]
}

// Level returns the current delay factor.
func (t *Throttler) Level() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.delayFactor
}
