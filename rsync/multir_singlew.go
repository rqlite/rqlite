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
	readReq   chan bool
	writeReq  chan bool
	readDone  chan bool
	writeDone chan bool

	wg   sync.WaitGroup
	done chan struct{}
}

// NewMultiRSW creates a new MultiRSW instance.
func NewMultiRSW() *MultiRSW {
	r := &MultiRSW{
		readReq:   make(chan bool),
		writeReq:  make(chan bool),
		readDone:  make(chan bool),
		writeDone: make(chan bool),
		done:      make(chan struct{}),
	}
	r.wg.Add(1)
	go r.manage()
	return r
}

// Close shuts down the MultiRSW instance.
func (r *MultiRSW) Close() {
	close(r.done)
	r.wg.Wait()
}

// BeginRead attempts to enter the critical section as a reader.
func (r *MultiRSW) BeginRead() error {
	r.readReq <- true
	if !<-r.readReq {
		return ErrMRSWConflict
	}
	return nil
}

// EndRead exits the critical section as a reader.
func (r *MultiRSW) EndRead() {
	r.readDone <- true
}

// BeginWrite attempts to enter the critical section as a writer.
func (r *MultiRSW) BeginWrite() error {
	r.writeReq <- true
	if !<-r.writeReq {
		return ErrMRSWConflict
	}
	return nil
}

// EndWrite exits the critical section as a writer.
func (r *MultiRSW) EndWrite() {
	r.writeDone <- true
}

func (r *MultiRSW) manage() {
	var readerCount int
	writerActive := false
	defer r.wg.Done()

	for {
		select {
		case <-r.readReq:
			if !writerActive {
				readerCount++
				r.readReq <- true
			} else {
				r.readReq <- false
			}
		case <-r.writeReq:
			if readerCount == 0 && !writerActive {
				writerActive = true
				r.writeReq <- true
			} else {
				r.writeReq <- false
			}
		case <-r.readDone:
			readerCount--
			if readerCount < 0 {
				panic("reader count went negative")
			}
		case <-r.writeDone:
			if !writerActive {
				panic("write done received but no write is active")
			}
			writerActive = false
		case <-r.done:
			return
		}
	}
}
