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

	c      chan *command.Statement
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
	}

	go q.run()
	return q
}

func (q *Queue) Write(stmt *command.Statement) error {
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

func (q *Queue) run() {
	defer close(q.closed)
	stmts := make([]*command.Statement, 0)
	//ticker := time.NewTicker(q.timeout)

	writeFn := func(stmts []*command.Statement) {
		q.exec(stmts)
		stmts = make([]*command.Statement, 0)
	}

	for {
		select {
		case s := <-q.c:
			stmts = append(stmts, s)
			if len(stmts) == q.batchSize {
				writeFn(stmts)
			}
		// case <-ticker.C:
		// 	// check batch, flush if empty. Not quite right. Timeout should expire when first item added?
		case <-q.flush:
			writeFn(stmts)
		case <-q.done:
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
