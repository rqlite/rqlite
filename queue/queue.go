package queue

import (
	"time"

	"github.com/rqlite/rqlite/command"
)

type Execer interface {
	Execute(er *command.ExecuteRequest) ([]*command.ExecuteResult, error)
}

type Queue struct {
	maxSize   int
	batchSize int
	timeout   time.Duration
	store     Execer

	c chan *command.Statement

	done   chan struct{}
	closed chan struct{}
	flush  chan struct{}
}

func New(maxSize, batchSize int, t time.Duration, e Execer) *Queue {
	q := &Queue{
		maxSize:   maxSize,
		batchSize: batchSize,
		timeout:   t,
		store:     e,
		c:         make(chan *command.Statement, maxSize),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		flush:     make(chan struct{}),
	}

	go q.run()
	return q
}

// Requests or ExecuteRequests? Gotta be requests, and merge inside single ER. Maybe just
// needs to be Statements
func (q *Queue) Write(stmt *command.Statement) error {
	if stmt == nil {
		return nil
	}
	q.c <- stmt
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
	return len(q.c)
}

func (q *Queue) run() {
	defer close(q.closed)
	var stmts []*command.Statement
	timer := time.NewTimer(q.timeout)
	timer.Stop()

	writeFn := func(stmts []*command.Statement) {
		q.exec(stmts)
		stmts = nil
		timer.Stop()
	}

	for {
		select {
		case s := <-q.c:
			stmts = append(stmts, s)
			if len(stmts) == 1 {
				timer.Reset(q.timeout)
			}
			if len(stmts) == q.batchSize {
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

func (q *Queue) exec(stmts []*command.Statement) error {
	if stmts == nil || len(stmts) == 0 {
		return nil
	}

	er := &command.ExecuteRequest{
		Request: &command.Request{
			Statements: stmts,
		},
	}

	// Doesn't handle leader-redirect, transparent forwarding, etc.
	// Would need a "wrapped" store which handles it.
	_, err := q.store.Execute(er)
	return err
}
