package rsync

import (
	"fmt"
	"sync"
)

// ErrMRSWConflict is returned when a MultiRSW operation fails.
type ErrMRSWConflict struct {
	msg string
}

// Error implements the error interface for ErrMRSWConflict.
func (e *ErrMRSWConflict) Error() string {
	return e.msg
}

// NewErrMRSWConflict creates a new ErrMRSWConflict with the given message.
func NewErrMRSWConflict(m string) error {
	return &ErrMRSWConflict{msg: m}
}

// MultiRSW is a simple concurrency control mechanism that allows
// multiple readers or a single writer to execute a critical section at a time.
type MultiRSW struct {
	owner      string
	numReaders int
	mu         sync.Mutex
	cond       *sync.Cond
}

// NewMultiRSW creates a new MultiRSW instance.
func NewMultiRSW() *MultiRSW {
	r := &MultiRSW{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// BeginRead attempts to enter the critical section as a reader.
func (r *MultiRSW) BeginRead() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.owner != "" {
		return NewErrMRSWConflict("MSRW conflict owner: " + r.owner)
	}
	r.numReaders++
	return nil
}

// EndRead exits the critical section as a reader.
func (r *MultiRSW) EndRead() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.numReaders--
	if r.numReaders < 0 {
		panic("reader count went negative")
	}
	if r.numReaders == 0 {
		r.cond.Broadcast()
	}
}

// BeginWrite attempts to enter the critical section as a writer.
func (r *MultiRSW) BeginWrite(owner string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner == "" {
		panic("owner cannot be empty")
	}
	if r.owner != "" {
		return NewErrMRSWConflict("MSRW conflict owner: " + r.owner)
	}
	if r.numReaders > 0 {
		return NewErrMRSWConflict(fmt.Sprintf("MSRW conflict %d readers active", r.numReaders))
	}
	r.owner = owner
	return nil
}

// BeginWriteBlocking enters the critical section as a writer, blocking until
// neither a writer nor any readers are active. The done channel can be used
// to cancel the wait; if done is closed before the lock is acquired, an
// ErrMRSWConflict is returned.
func (r *MultiRSW) BeginWriteBlocking(owner string, done <-chan struct{}) error {
	if owner == "" {
		panic("owner cannot be empty")
	}

	// Goroutine to wake us via Broadcast if done is closed while
	// we are blocked in cond.Wait().
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-done:
			r.cond.Broadcast()
		case <-stop:
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()
	for r.owner != "" || r.numReaders > 0 {
		select {
		case <-done:
			return NewErrMRSWConflict("MSRW write wait cancelled")
		default:
		}
		r.cond.Wait()
	}
	select {
	case <-done:
		return NewErrMRSWConflict("MSRW write wait cancelled")
	default:
	}
	r.owner = owner
	return nil
}

// EndWrite exits the critical section as a writer.
func (r *MultiRSW) EndWrite() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.owner == "" {
		panic("write done received but no write is active")
	}
	r.owner = ""
	r.cond.Broadcast()
}

// UpgradeToWriter attempts to upgrade a read lock to a write lock. The
// client must be the only reader in order to upgrade, and must already
// be in a read lock.
func (r *MultiRSW) UpgradeToWriter(owner string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.owner != "" {
		return NewErrMRSWConflict("MSRW conflict owner: " + r.owner)
	}
	if r.numReaders > 1 {
		return NewErrMRSWConflict(fmt.Sprintf("MSRW conflict %d readers active", r.numReaders))
	}
	if r.numReaders == 0 {
		panic("upgrade attempted with no readers")
	}
	r.owner = owner
	r.numReaders = 0
	return nil
}
