package queue

import (
	"time"

	"github.com/rqlite/rqlite/command"
)

type Queue struct {
	maxSize   int
	batchSize int
	timeout   time.Duration

	batchCh chan *command.Statement
	sendCh  chan []*command.Statement
	C       <-chan []*command.Statement

	done   chan struct{}
	closed chan struct{}
	flush  chan struct{}
}

func New(maxSize, batchSize int, t time.Duration) *Queue {
	q := &Queue{
		maxSize:   maxSize,
		batchSize: batchSize,
		timeout:   t,
		batchCh:   make(chan *command.Statement, maxSize),
		sendCh:    make(chan []*command.Statement, maxSize),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		flush:     make(chan struct{}),
	}

	q.C = q.sendCh
	go q.run()
	return q
}

// Requests or ExecuteRequests? Gotta be requests, and merge inside single ER. Maybe just
// needs to be Statements
func (q *Queue) Write(stmt *command.Statement) error {
	if stmt == nil {
		return nil
	}
	q.batchCh <- stmt
	return nil
}

func (q *Queue) Flush() error {
	q.flush <- struct{}{}
	return nil
}

func (q *Queue) Close() error {
	close(q.done)
	<-q.closed
	return nil
}

func (q *Queue) Depth() int {
	return len(q.batchCh)
}

func (q *Queue) run() {
	defer close(q.closed)
	var stmts []*command.Statement
	timer := time.NewTimer(q.timeout)
	timer.Stop()

	writeFn := func(stmts []*command.Statement) {
		newStmts := make([]*command.Statement, len(stmts))
		copy(newStmts, stmts)
		q.sendCh <- newStmts

		stmts = nil
		timer.Stop()
	}

	for {
		select {
		case s := <-q.batchCh:
			stmts = append(stmts, s)
			if len(stmts) == 1 {
				timer.Reset(q.timeout)
			}
			if len(stmts) >= q.batchSize {
				writeFn(stmts)
			}
		case <-timer.C:
			writeFn(stmts)
		case <-q.flush:
			writeFn(stmts)
		case <-q.done:
			timer.Stop()
			return
		}
	}
}
