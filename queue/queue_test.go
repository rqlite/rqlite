package queue

import (
	"errors"
	"reflect"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

var (
	testStmtFoo        = &command.Statement{Sql: "SELECT * FROM foo"}
	testStmtBar        = &command.Statement{Sql: "SELECT * FROM bar"}
	testStmtsFoo       = []*command.Statement{testStmtFoo}
	testStmtsBar       = []*command.Statement{testStmtBar}
	testStmtsFooBar    = []*command.Statement{testStmtFoo, testStmtBar}
	testStmtsFooBarFoo = []*command.Statement{testStmtFoo, testStmtBar, testStmtFoo}
	flushChan1         = make(FlushChannel)
	flushChan2         = make(FlushChannel)
)

func Test_MergeQueuedObjects(t *testing.T) {
	if mergeQueued[*command.Statement](nil) != nil {
		t.Fatalf("merging of nil failed")
	}

	tests := []struct {
		qs  []*queuedObjects[*command.Statement]
		exp *Request[*command.Statement]
	}{
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, nil, flushChan1},
			},
			exp: &Request[*command.Statement]{1, nil, []FlushChannel{flushChan1}},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, nil, flushChan1},
				{2, testStmtsFoo, nil},
			},
			exp: &Request[*command.Statement]{2, testStmtsFoo, []FlushChannel{flushChan1}},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, testStmtsFoo, nil},
			},
			exp: &Request[*command.Statement]{1, testStmtsFoo, nil},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, testStmtsFoo, nil},
				{2, testStmtsBar, nil},
			},
			exp: &Request[*command.Statement]{2, testStmtsFooBar, nil},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, testStmtsFooBar, nil},
				{2, testStmtsFoo, nil},
			},
			exp: &Request[*command.Statement]{2, testStmtsFooBarFoo, nil},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, testStmtsFooBar, flushChan1},
				{2, testStmtsFoo, flushChan2},
			},
			exp: &Request[*command.Statement]{2, testStmtsFooBarFoo, []FlushChannel{flushChan1, flushChan2}},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{1, testStmtsFooBar, nil},
				{2, testStmtsFoo, flushChan2},
			},
			exp: &Request[*command.Statement]{2, testStmtsFooBarFoo, []FlushChannel{flushChan2}},
		},
		{
			qs: []*queuedObjects[*command.Statement]{
				{2, testStmtsFooBar, nil},
				{1, testStmtsFoo, flushChan2},
			},
			exp: &Request[*command.Statement]{2, testStmtsFooBarFoo, []FlushChannel{flushChan2}},
		},
	}

	for i, tt := range tests {
		r := mergeQueued(tt.qs)
		if got, exp := r.SequenceNumber, tt.exp.SequenceNumber; got != exp {
			t.Fatalf("incorrect sequence number for test %d, exp %d, got %d", i, exp, got)
		}
		if !reflect.DeepEqual(r.Objects, tt.exp.Objects) {
			t.Fatalf("statements don't match for test %d", i)
		}
		if len(r.flushChans) != len(tt.exp.flushChans) {
			t.Fatalf("incorrect number of flush channels for test %d", i)
		}
		for j := range r.flushChans {
			if r.flushChans[j] != tt.exp.flushChans[j] {
				t.Fatalf("wrong channel for test %d", i)
			}
		}
	}
}

func Test_QueueNew(t *testing.T) {
	q := New[*command.Statement](1, 1, 100*time.Millisecond)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	defer q.Close()
}

func Test_QueueClosedWrite(t *testing.T) {
	q := New[*command.Statement](1, 1, 100*time.Millisecond)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	q.Close()
	if _, err := q.Write(testStmtsFoo, nil); err == nil {
		t.Fatalf("failed to detect closed queue")
	}
}

// Test_QueueCloseUnreadConsumer ensures that Close returns even when the
// consumer of C never reads, and the batching goroutine is blocked
// handing off a batch.
func Test_QueueCloseUnreadConsumer(t *testing.T) {
	q := New[*command.Statement](1, 1, 0)

	// First batch is buffered on C, second batch blocks the batcher
	// handing it off, since nothing reads C.
	for range 2 {
		if _, err := q.Write(testStmtsFoo, nil); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	waitUntil(t, func() bool { return len(q.batchCh) == 0 })

	closed := make(chan struct{})
	go func() {
		q.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Close with unread consumer")
	}
}

// Test_QueueCloseBlockedWrite ensures that a Write blocked on a full
// queue returns ErrClosed when the queue is closed.
func Test_QueueCloseBlockedWrite(t *testing.T) {
	q := New[*command.Statement](1, 1, 0)

	// Fill the batcher's output, the batcher's handoff, and the
	// queue's own buffer. Nothing reads C so none of it drains.
	for range 3 {
		if _, err := q.Write(testStmtsFoo, nil); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	waitUntil(t, func() bool { return len(q.batchCh) == 1 })

	writeErr := make(chan error)
	go func() {
		_, err := q.Write(testStmtsBar, nil)
		writeErr <- err
	}()
	select {
	case err := <-writeErr:
		t.Fatalf("write on full queue returned before close: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	q.Close()
	select {
	case err := <-writeErr:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed from blocked write, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for blocked write to return after close")
	}
}

// Test_QueueCloseBlockedFlush ensures that a Flush blocked on a full
// queue returns ErrClosed when the queue is closed.
func Test_QueueCloseBlockedFlush(t *testing.T) {
	q := New[*command.Statement](1, 1, 0)

	for range 3 {
		if _, err := q.Write(testStmtsFoo, nil); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	waitUntil(t, func() bool { return len(q.batchCh) == 1 })

	flushErr := make(chan error)
	go func() {
		flushErr <- q.Flush()
	}()
	select {
	case err := <-flushErr:
		t.Fatalf("flush on full queue returned before close: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	q.Close()
	select {
	case err := <-flushErr:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed from blocked flush, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for blocked flush to return after close")
	}
}

func Test_QueueWriteNil(t *testing.T) {
	q := New[*command.Statement](1, 1, 60*time.Second)
	defer q.Close()

	if _, err := q.Write(nil, nil); err != nil {
		t.Fatalf("failing to write nil: %s", err.Error())
	}
}

func Test_QueueWriteOne(t *testing.T) {
	q := New[*command.Statement](1024, 1, 60*time.Second)
	defer q.Close()

	if _, err := q.WriteOne(testStmtFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		if !reflect.DeepEqual(req.Objects[0], testStmtFoo) {
			t.Fatalf("received wrong statement, got: %v, want: %v", req.Objects[0], testStmtFoo)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

func Test_QueueWriteOne_FlushEmpty(t *testing.T) {
	q := New[*command.Statement](1024, 2, 60*time.Second)
	defer q.Close()

	if err := q.Flush(); err != nil {
		t.Fatalf("failed to flush: %s", err.Error())
	}

	select {
	case req := <-q.C:
		t.Fatalf("received unexpected request on empty flush: %v", req)
	case <-time.After(100 * time.Millisecond):
		// Expected, nothing to receive.
	}
}

func Test_QueueWriteOne_Flush(t *testing.T) {
	q := New[*command.Statement](1024, 2, 60*time.Second)
	defer q.Close()

	if _, err := q.WriteOne(testStmtFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	if err := q.Flush(); err != nil {
		t.Fatalf("failed to flush: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		if !reflect.DeepEqual(req.Objects[0], testStmtFoo) {
			t.Fatalf("received wrong statement, got: %v, want: %v", req.Objects[0], testStmtFoo)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

func Test_QueueWriteBatchSizeSingle(t *testing.T) {
	q := New[*command.Statement](1024, 1, 60*time.Second)
	defer q.Close()

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

func Test_QueueWriteBatchSizeDouble(t *testing.T) {
	q := New[*command.Statement](1024, 1, 60*time.Second)
	defer q.Close()

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	if _, err := q.Write(testStmtsBar, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	// Just test that I get a batch size, each time.
	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		if req.Objects[0].Sql != "SELECT * FROM bar" {
			t.Fatalf("received wrong SQL")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

func Test_QueueWriteNilAndOne(t *testing.T) {
	q := New[*command.Statement](1024, 2, 60*time.Second)
	defer q.Close()

	if _, err := q.Write(nil, nil); err != nil {
		t.Fatalf("failed to write nil: %s", err.Error())
	}
	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		req.Close()
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

func Test_QueueWriteBatchSizeSingleChan(t *testing.T) {
	q := New[*command.Statement](1024, 1, 60*time.Second)
	defer q.Close()

	fc := make(FlushChannel)
	if _, err := q.Write(testStmtsFoo, fc); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if exp, got := 1, len(req.Objects); exp != got {
			t.Fatalf("received wrong length slice, exp %d, got %d", exp, got)
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
		req.Close()
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}

	select {
	case <-fc:
		// nothing to do.
	default:
		// Not closed, something is wrong.
		t.Fatalf("flush channel not closed")
	}
}

func Test_QueueWriteNilSingleChan(t *testing.T) {
	q := New[*command.Statement](1024, 1, 60*time.Second)
	defer q.Close()

	fc := make(FlushChannel)
	if _, err := q.Write(nil, fc); err != nil {
		t.Fatalf("failed to write nil: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if req.Objects != nil {
			t.Fatalf("statements slice is not nil")
		}
		if len(req.flushChans) != 1 || req.flushChans[0] != fc {
			t.Fatalf("flush chans is not correct")
		}
		req.Close()
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}

	select {
	case <-fc:
		// nothing to do.
	default:
		// Not closed, something is wrong.
		t.Fatalf("flush channel not closed")
	}
}

func Test_QueueWriteBatchSizeMulti(t *testing.T) {
	q := New[*command.Statement](1024, 5, 60*time.Second)
	defer q.Close()

	// Write a batch size and wait for it.
	for range 5 {
		if _, err := q.Write(testStmtsFoo, nil); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	select {
	case req := <-q.C:
		if len(req.Objects) != 5 {
			t.Fatalf("received wrong length slice")
		}
		if q.numTimeouts != 0 {
			t.Fatalf("queue timeout expired?")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for first statements")
	}

	// Write one more than a batch size, should still get a batch.
	for range 6 {
		if _, err := q.Write(testStmtsBar, nil); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	select {
	case req := <-q.C:
		if len(req.Objects) < 5 {
			t.Fatalf("received too-short slice")
		}
		if q.numTimeouts != 0 {
			t.Fatalf("queue timeout expired?")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for second statements")
	}
}

func Test_QueueWriteTimeout(t *testing.T) {
	q := New[*command.Statement](1024, 10, 1*time.Second)
	defer q.Close()

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if len(req.Objects) != 1 {
			t.Fatalf("received wrong length slice")
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
		if q.numTimeouts != 1 {
			t.Fatalf("queue timeout didn't expire")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

// Test_QueueWriteTimeoutMulti ensures that the timer expiring
// twice in a row works fine.
func Test_QueueWriteTimeoutMulti(t *testing.T) {
	q := New[*command.Statement](1024, 10, 1*time.Second)
	defer q.Close()

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	select {
	case req := <-q.C:
		if len(req.Objects) != 1 {
			t.Fatalf("received wrong length slice")
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
		if q.numTimeouts != 1 {
			t.Fatalf("queue timeout didn't expire")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for first statement")
	}

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	select {
	case req := <-q.C:
		if len(req.Objects) != 1 {
			t.Fatalf("received wrong length slice")
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
		if q.numTimeouts != 2 {
			t.Fatalf("queue timeout didn't expire")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for second statement")
	}
}

// Test_QueueWriteTimeoutBatch ensures that the timer expiring
// followed by a batch works fine.
func Test_QueueWriteTimeoutBatch(t *testing.T) {
	q := New[*command.Statement](1024, 2, 1*time.Second)
	defer q.Close()

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case req := <-q.C:
		if len(req.Objects) != 1 {
			t.Fatalf("received wrong length slice")
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
		if q.numTimeouts != 1 {
			t.Fatalf("queue timeout didn't expire")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}

	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	if _, err := q.Write(testStmtsFoo, nil); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	select {
	case req := <-q.C:
		// Should happen before the timeout expires.
		if len(req.Objects) != 2 {
			t.Fatalf("received wrong length slice")
		}
		if req.Objects[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
		if q.numTimeouts != 1 {
			t.Fatalf("queue timeout expired?")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for statement")
	}
}

type testObj struct {
	id int
}

// Test_QueueOrdering ensures that ordering is preserved in the queue.
func Test_QueueOrdering(t *testing.T) {
	batchTimeout := 100 * time.Millisecond
	testTimeout := 3 * batchTimeout

	q := New[*testObj](100, 13, batchTimeout)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	defer q.Close()

	num := 1063
	done := make(chan struct{})
	go func() {
		expIdx := 0
		for req := range q.C {
			for _, r := range req.Objects {
				if r.id != expIdx {
					t.Errorf("out of order: exp %d, got %d", expIdx, r.id)
					return
				}
				expIdx++
			}
			req.Close()
			if expIdx == num {
				close(done)
				return
			}
		}
	}()

	for i := range num {
		if _, err := q.Write([]*testObj{{i}}, nil); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for all indexes")
	}
}

// waitUntil waits until fn returns true, failing the test if that
// takes too long.
func waitUntil(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for condition")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
