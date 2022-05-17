package queue

import (
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
)

var testStmt = &command.Statement{
	Sql: "SELECT * FROM foo",
}

func Test_NewQueue(t *testing.T) {
	q := New(1, 1, 100*time.Millisecond)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	defer q.Close()
}

func Test_NewQueueWriteNil(t *testing.T) {
	q := New(1, 1, 60*time.Second)
	defer q.Close()

	if err := q.Write(nil); err != nil {
		t.Fatalf("failing to write nil: %s", err.Error())
	}
}

func Test_NewQueueWriteBatchSizeSingle(t *testing.T) {
	q := New(1024, 1, 60*time.Second)
	defer q.Close()

	if err := q.Write(testStmt); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case stmts := <-q.C:
		if len(stmts) != 1 {
			t.Fatalf("received wrong length slice")
		}
		if stmts[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatalf("timed out waiting for statement")
	}
}

func Test_NewQueueWriteBatchSizeMulti(t *testing.T) {
	q := New(1024, 5, 60*time.Second)
	defer q.Close()

	// Write a batch size and wait for it.
	for i := 0; i < 5; i++ {
		if err := q.Write(testStmt); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	select {
	case stmts := <-q.C:
		if len(stmts) != 5 {
			t.Fatalf("received wrong length slice")
		}
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatalf("timed out waiting for first statements")
	}

	// Write one more than a batch size, should still get a batch.
	for i := 0; i < 6; i++ {
		if err := q.Write(testStmt); err != nil {
			t.Fatalf("failed to write: %s", err.Error())
		}
	}
	select {
	case stmts := <-q.C:
		if len(stmts) < 5 {
			t.Fatalf("received too-short slice")
		}
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatalf("timed out waiting for second statements")
	}
}

func Test_NewQueueWriteTimeout(t *testing.T) {
	q := New(1024, 10, 1*time.Second)
	defer q.Close()

	if err := q.Write(testStmt); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	select {
	case stmts := <-q.C:
		if len(stmts) != 1 {
			t.Fatalf("received wrong length slice")
		}
		if stmts[0].Sql != "SELECT * FROM foo" {
			t.Fatalf("received wrong SQL")
		}
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatalf("timed out waiting for statement")
	}
}
