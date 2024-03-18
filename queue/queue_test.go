package queue

import (
	"reflect"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v8/command/proto"
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
		for i := range r.flushChans {
			if r.flushChans[i] != tt.exp.flushChans[i] {
				t.Fatalf("wrong channel for test %d", i)
			}
		}
	}
}

func Test_NewQueue(t *testing.T) {
	q := New[*command.Statement](1, 1, 100*time.Millisecond)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	defer q.Close()
}

func Test_NewQueueClosedWrite(t *testing.T) {
	q := New[*command.Statement](1, 1, 100*time.Millisecond)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	q.Close()
	if _, err := q.Write(testStmtsFoo, nil); err == nil {
		t.Fatalf("failed to detect closed queue")
	}
}

func Test_NewQueueWriteNil(t *testing.T) {
	q := New[*command.Statement](1, 1, 60*time.Second)
	defer q.Close()

	if _, err := q.Write(nil, nil); err != nil {
		t.Fatalf("failing to write nil: %s", err.Error())
	}
}

func Test_NewQueueWriteBatchSizeSingle(t *testing.T) {
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

func Test_NewQueueWriteBatchSizeDouble(t *testing.T) {
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

func Test_NewQueueWriteNilAndOne(t *testing.T) {
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

func Test_NewQueueWriteBatchSizeSingleChan(t *testing.T) {
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

func Test_NewQueueWriteNilSingleChan(t *testing.T) {
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
		if len(req.flushChans) != 1 && req.flushChans[0] != fc {
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

func Test_NewQueueWriteBatchSizeMulti(t *testing.T) {
	q := New[*command.Statement](1024, 5, 60*time.Second)
	defer q.Close()

	// Write a batch size and wait for it.
	for i := 0; i < 5; i++ {
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
	for i := 0; i < 6; i++ {
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

func Test_NewQueueWriteTimeout(t *testing.T) {
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

// Test_NewQueueWriteTimeoutMulti ensures that timer expiring
// twice in a row works fine.
func Test_NewQueueWriteTimeoutMulti(t *testing.T) {
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

// Test_NewQueueWriteTimeoutBatch ensures that timer expiring
// followed by a batch, works fine.
func Test_NewQueueWriteTimeoutBatch(t *testing.T) {
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
