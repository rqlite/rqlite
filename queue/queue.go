package queue

import (
	"errors"
	"expvar"
	"sync"
	"time"
)

// stats captures stats for the Queue.
var stats *expvar.Map

const (
	numObjectsRx = "objects_rx"
	numObjectsTx = "objects_tx"
	numTimeout   = "num_timeout"
	numFlush     = "num_flush"
)

func init() {
	stats = expvar.NewMap("queue")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numObjectsRx, 0)
	stats.Add(numObjectsTx, 0)
	stats.Add(numTimeout, 0)
	stats.Add(numFlush, 0)
}

// FlushChannel is the type passed to the Queue, if caller wants
// to know when a specific set of objects has been processed.
type FlushChannel chan bool

// Request represents a batch of objects to be processed.
type Request[T any] struct {
	SequenceNumber int64
	Objects        []T
	flushChans     []FlushChannel
}

// Close closes a request, closing any associated flush channels.
func (r *Request[T]) Close() {
	for _, c := range r.flushChans {
		close(c)
	}
}

type queuedObjects[T any] struct {
	SequenceNumber int64
	Objects        []T
	flushChan      FlushChannel
}

func mergeQueued[T any](qs []*queuedObjects[T]) *Request[T] {
	var o *Request[T]
	if len(qs) > 0 {
		o = &Request[T]{
			SequenceNumber: qs[0].SequenceNumber,
			flushChans:     make([]FlushChannel, 0),
		}
	}

	for i := range qs {
		if o.SequenceNumber < qs[i].SequenceNumber {
			o.SequenceNumber = qs[i].SequenceNumber
		}
		o.Objects = append(o.Objects, qs[i].Objects...)
		if qs[i].flushChan != nil {
			o.flushChans = append(o.flushChans, qs[i].flushChan)
		}
	}
	return o
}

// Queue is a batching queue with a timeout.
type Queue[T any] struct {
	maxSize   int
	batchSize int
	timeout   time.Duration

	batchCh chan *queuedObjects[T]

	sendCh chan *Request[T]
	C      <-chan *Request[T]

	done   chan struct{}
	closed chan struct{}
	flush  chan struct{}

	seqMu  sync.Mutex
	seqNum int64

	// Whitebox unit-testing
	numTimeouts int
}

// New returns a instance of a Queue
func New[T any](maxSize, batchSize int, t time.Duration) *Queue[T] {
	q := &Queue[T]{
		maxSize:   maxSize,
		batchSize: batchSize,
		timeout:   t,
		batchCh:   make(chan *queuedObjects[T], maxSize),
		sendCh:    make(chan *Request[T], 1),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		flush:     make(chan struct{}),
		seqNum:    time.Now().UnixNano(),
	}

	q.C = q.sendCh
	go q.run()
	return q
}

// Write queues a request, and returns a monotonically increasing
// sequence number associated with the slice of objects. A slice with
// a lower sequence number than second slice will always be transmitted
// on the Queue's C object before the second slice.
//
// c is an optional channel. If non-nil, it will be closed when the Request
// containing these objects is closed. Normally this close operation should
// be called by whatever is processing the objects read from the queue, to
// indicate that the objects have been processed. The canoncial use of this
// is to allow the caller to block until the objects are processed.
func (q *Queue[T]) Write(objects []T, c FlushChannel) (int64, error) {
	select {
	case <-q.done:
		return 0, errors.New("queue is closed")
	default:
	}

	// Take the lock and don't release it until the function returns.
	// This ensures that the incremented sequence number and write to
	// batch channel are synchronized.
	q.seqMu.Lock()
	defer q.seqMu.Unlock()
	q.seqNum++

	q.batchCh <- &queuedObjects[T]{
		SequenceNumber: q.seqNum,
		Objects:        objects,
		flushChan:      c,
	}
	stats.Add(numObjectsRx, int64(len(objects)))
	return q.seqNum, nil
}

// Flush flushes the queue
func (q *Queue[T]) Flush() error {
	q.flush <- struct{}{}
	return nil
}

// Close closes the queue. A closed queue should not be used.
func (q *Queue[T]) Close() error {
	select {
	case <-q.done:
	default:
		close(q.done)
		<-q.closed
	}
	return nil
}

// Depth returns the number of queued requests
func (q *Queue[T]) Depth() int {
	return len(q.batchCh)
}

// Stats returns stats on this queue.
func (q *Queue[T]) Stats() (map[string]any, error) {
	return map[string]any{
		"max_size":   q.maxSize,
		"batch_size": q.batchSize,
		"timeout":    q.timeout.String(),
	}, nil
}

func (q *Queue[T]) run() {
	defer close(q.closed)

	queuedStmts := make([]*queuedObjects[T], 0)
	// Create an initial timer, in the stopped state.
	timer := time.NewTimer(0)
	<-timer.C

	writeFn := func() {
		// mergeQueued returns a new object, ownership will pass
		// implicitly to the other side of sendCh.
		req := mergeQueued(queuedStmts)
		if req != nil {
			q.sendCh <- req
			stats.Add(numObjectsTx, int64(len(req.Objects)))
			queuedStmts = queuedStmts[:0] // Better on the GC than setting to nil.
		}
	}

	for {
		select {
		case s := <-q.batchCh:
			queuedStmts = append(queuedStmts, s)
			if len(queuedStmts) == 1 {
				// First item in queue, start the timer so that if
				// we don't get in a batch, we'll still write.
				timer.Reset(q.timeout)
			}
			if len(queuedStmts) == q.batchSize {
				stopTimer(timer)
				writeFn()
			}
		case <-timer.C:
			stats.Add(numTimeout, 1)
			q.numTimeouts++
			writeFn()
		case <-q.flush:
			stats.Add(numFlush, 1)
			stopTimer(timer)
			writeFn()
		case <-q.done:
			stopTimer(timer)
			return
		}
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() && len(timer.C) > 0 {
		<-timer.C
	}
}
