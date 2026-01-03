package rsync

import (
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
func NewErrMRSWConflict(msg string) error {
	return &ErrMRSWConflict{msg: msg}
}

// MultiRSW is a simple concurrency control mechanism that allows
// multiple readers or a single writer to execute a critical section at a time.
type MultiRSW struct {
	writerActive bool
	numReaders   int
	mu           sync.Mutex
}

// NewMultiRSW creates a new MultiRSW instance.
func NewMultiRSW() *MultiRSW {
	return &MultiRSW{}
}

// BeginRead attempts to enter the critical section as a reader.
func (r *MultiRSW) BeginRead() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writerActive {
		return NewErrMRSWConflict("MSRW conflict")
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
}

// BeginWrite attempts to enter the critical section as a writer.
func (r *MultiRSW) BeginWrite() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writerActive || r.numReaders > 0 {
		return NewErrMRSWConflict("MSRW conflict")
	}
	r.writerActive = true
	return nil
}

// EndWrite exits the critical section as a writer.
func (r *MultiRSW) EndWrite() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.writerActive {
		panic("write done received but no write is active")
	}
	r.writerActive = false
}

// UpgradeToWriter attempts to upgrade a read lock to a write lock. The
// client must be the only reader in order to upgrade, and must already
// be in a read lock.
func (r *MultiRSW) UpgradeToWriter() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writerActive || r.numReaders > 1 {
		return NewErrMRSWConflict("MSRW conflict")
	}
	if r.numReaders == 0 {
		panic("upgrade attempted with no readers")
	}
	r.writerActive = true
	r.numReaders = 0
	return nil
}
