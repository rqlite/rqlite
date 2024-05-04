package rsync

import (
	"context"
	"errors"
)

var (
	// ErrMRSWConflict is returned when a MultiRSW operation fails.
	ErrMRSWConflict = errors.New("MRSW conflict")
)

type MultiRSW struct {
	readReq   chan bool
	writeReq  chan bool
	readDone  chan bool
	writeDone chan bool
	ctx       context.Context
}

func NewMultiRSW(ctx context.Context) *MultiRSW {
	r := &MultiRSW{
		readReq:   make(chan bool),
		writeReq:  make(chan bool),
		readDone:  make(chan bool),
		writeDone: make(chan bool),
		ctx:       ctx,
	}
	go r.manage()
	return r
}

func (r *MultiRSW) manage() {
	var readerCount int
	writerActive := false

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
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *MultiRSW) BeginRead() error {
	r.readReq <- true
	if !<-r.readReq {
		return ErrMRSWConflict
	}
	return nil
}

func (r *MultiRSW) EndRead() {
	r.readDone <- true
}

func (r *MultiRSW) BeginWrite() error {
	r.writeReq <- true
	if !<-r.writeReq {
		return ErrMRSWConflict
	}
	return nil
}

func (r *MultiRSW) EndWrite() {
	r.writeDone <- true
}
