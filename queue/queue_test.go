package queue

import (
	"sync"
	"testing"
	"time"

	"github.com/rqlite/rqlite/command"
)

var testStmt = &command.Statement{
	Sql: "SELECT * FROM foo",
}

func Test_NewQueue(t *testing.T) {
	q := New(1, 1, 100*time.Millisecond, nil)
	if q == nil {
		t.Fatalf("failed to create new Queue")
	}
	defer q.Close()
}

func Test_NewQueueWriteNil(t *testing.T) {
	m := &MockExecer{}
	q := New(1, 1, 60*time.Second, m)
	defer q.Close()

	if err := q.Write(nil); err != nil {
		t.Fatalf("failing to write nil: %s", err.Error())
	}
}

func Test_NewQueueWriteBatchSize(t *testing.T) {
	m := &MockExecer{}
	q := New(1024, 1, 60*time.Second, m)
	defer q.Close()

	var wg sync.WaitGroup
	var numExecs int
	wg.Add(1)
	m.execFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		wg.Done()
		numExecs++
		return nil, nil
	}

	if err := q.Write(testStmt); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}
	wg.Wait()

	if exp, got := 1, numExecs; exp != got {
		t.Fatalf("exec not called correct number of times, exp %d got %d", exp, got)
	}
}

func Test_NewQueueWriteFlush(t *testing.T) {
	m := &MockExecer{}
	q := New(1024, 10, 60*time.Second, m)
	defer q.Close()

	var wg sync.WaitGroup
	var numExecs int
	wg.Add(1)
	m.execFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		wg.Done()
		numExecs++
		return nil, nil
	}

	if err := q.Write(testStmt); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	time.Sleep(1 * time.Second)

	if err := q.Flush(); err != nil {
		t.Fatalf("failed to flush: %s", err.Error())
	}
	wg.Wait()

	if exp, got := 1, numExecs; exp != got {
		t.Fatalf("exec not called correct number of times, exp %d got %d", exp, got)
	}
}

func Test_NewQueueWriteTimeout(t *testing.T) {
	m := &MockExecer{}
	q := New(1024, 10, 1*time.Second, m)
	defer q.Close()

	var wg sync.WaitGroup
	var numExecs int
	wg.Add(1)
	m.execFn = func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
		wg.Done()
		numExecs++
		return nil, nil
	}

	if err := q.Write(testStmt); err != nil {
		t.Fatalf("failed to write: %s", err.Error())
	}

	time.Sleep(time.Second)

	wg.Wait()
	if exp, got := 1, numExecs; exp != got {
		t.Fatalf("exec not called correct number of times, exp %d got %d", exp, got)
	}
}

type MockExecer struct {
	execFn func(er *command.ExecuteRequest) ([]*command.ExecuteResult, error)
}

func (m *MockExecer) Execute(er *command.ExecuteRequest) ([]*command.ExecuteResult, error) {
	if m.execFn != nil {
		return m.execFn(er)
	}
	return nil, nil
}
