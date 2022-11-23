package queue

import (
	"expvar"
	"sync"
	"time"

	"github.com/rqlite/rqlite/command"
)

// stats captures stats for the Queue.
var stats *expvar.Map

const (
	numStatementsRx = "statements_rx"
	numStatementsTx = "statements_tx"
)

func init() {
	stats = expvar.NewMap("queue")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Set(numStatementsRx, 0)
	stats.Set(numStatementsTx, 0)
}

// FlushChannel is the type passed to the Queue, if caller wants
// to know when a specific set of statements has been processed.
type FlushChannel chan bool

// Request represents a batch of statements to be processed.
type Request struct {
	SequenceNumber int64
	Statements     []*command.Statement
	flushChans     []FlushChannel
}

// Close closes a request, closing any associated flush channels.
func (r *Request) Close() {
	for _, c := range r.flushChans {
		close(c)
	}
}

type queuedStatements struct {
	SequenceNumber int64
	Statements     []*command.Statement
	flushChan      FlushChannel
}

func mergeQueued(qs []*queuedStatements) *Request {
	var o *Request
	for i := range qs {
		if o == nil {
			o = &Request{
				SequenceNumber: qs[i].SequenceNumber,
				flushChans:     make([]FlushChannel, 0),
			}
		} else {
			if o.SequenceNumber < qs[i].SequenceNumber {
				o.SequenceNumber = qs[i].SequenceNumber
			}
		}
		o.Statements = append(o.Statements, qs[i].Statements...)
		if qs[i].flushChan != nil {
			o.flushChans = append(o.flushChans, qs[i].flushChan)
		}
	}
	return o
}

// Queue is a batching queue with a timeout.
type Queue struct {
	maxSize   int
	batchSize int
	timeout   time.Duration

	batchCh chan *queuedStatements

	sendCh chan *Request
	C      <-chan *Request

	done   chan struct{}
	closed chan struct{}
	flush  chan struct{}

	seqMu  sync.Mutex
	seqNum int64

	// Whitebox unit-testing
	numTimeouts int
}

// New returns a instance of a Queue
func New(maxSize, batchSize int, t time.Duration) *Queue {
	q := &Queue{
		maxSize:   maxSize,
		batchSize: batchSize,
		timeout:   t,
		batchCh:   make(chan *queuedStatements, maxSize),
		sendCh:    make(chan *Request, 1),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		flush:     make(chan struct{}),
		seqNum:    time.Now().UnixNano(),
	}

	q.C = q.sendCh
	go q.run()
	return q
}

// Write queues a request, and returns a monotonically incrementing
// sequence number associated with the slice of statements. If one
// slice has a larger sequence number than a number, the former slice
// will always be commited to Raft before the latter slice.
//
// c is an optional channel. If non-nil, it will be closed when the Request
// containing these statements is closed.
func (q *Queue) Write(stmts []*command.Statement, c FlushChannel) (int64, error) {
	q.seqMu.Lock()
	defer q.seqMu.Unlock()
	q.seqNum++

	stats.Add(numStatementsRx, int64(len(stmts)))
	q.batchCh <- &queuedStatements{
		SequenceNumber: q.seqNum,
		Statements:     stmts,
		flushChan:      c,
	}
	return q.seqNum, nil
}

// Flush flushes the queue
func (q *Queue) Flush() error {
	q.flush <- struct{}{}
	return nil
}

// Close closes the queue. A closed queue should not be used.
func (q *Queue) Close() error {
	select {
	case <-q.done:
	default:
		close(q.done)
		<-q.closed
	}
	return nil
}

// Depth returns the number of queued requests
func (q *Queue) Depth() int {
	return len(q.batchCh)
}

// Stats returns stats on this queue.
func (q *Queue) Stats() (map[string]interface{}, error) {
	return map[string]interface{}{
		"max_size":   q.maxSize,
		"batch_size": q.batchSize,
		"timeout":    q.timeout.String(),
	}, nil
}

func (q *Queue) run() {
	defer close(q.closed)

	queuedStmts := make([]*queuedStatements, 0)
	timer := time.NewTimer(q.timeout)
	timer.Stop()

	writeFn := func() {
		timer.Stop()
		if queuedStmts == nil {
			// Batch size was met, but timer expired before it could be
			// stopped, so this function was called again. Possibly.
			return
		}

		// mergeQueued returns a new object, ownership will pass
		// implicitly to the other side of sendCh.
		req := mergeQueued(queuedStmts)
		stats.Add(numStatementsTx, int64(len(req.Statements)))
		q.sendCh <- req
		queuedStmts = nil
	}

	for {
		select {
		case s := <-q.batchCh:
			queuedStmts = append(queuedStmts, s)
			if len(queuedStmts) == 1 {
				timer.Reset(q.timeout)
			}
			if len(queuedStmts) == q.batchSize {
				writeFn()
			}
		case <-timer.C:
			q.numTimeouts++
			writeFn()
		case <-q.flush:
			writeFn()
		case <-q.done:
			timer.Stop()
			return
		}
	}
}
