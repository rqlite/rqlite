package rsync

import (
	"errors"
	"sync"
)

var (
	// ErrMRSWConflict is returned when a MultiRSW operation fails.
	ErrMRSWConflict = errors.New("MRSW conflict")
)

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
		return ErrMRSWConflict
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
		return ErrMRSWConflict
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
