package sql

import (
	"testing"
)

func Test_ScannerNew(t *testing.T) {
	s := NewScanner(nil)
	if s == nil {
		t.Fatalf("failed to create basic Scanner")
	}
}

func Test_stackEmpty(t *testing.T) {
	s := newStack()
	if !s.empty() {
		t.Fatal("new stack is not empty")
	}

	if s.peek() != rune(0) {
		t.Fatal("peek of empty stack does not return correct value")
	}
}

func Test_stackSingle(t *testing.T) {
	s := newStack()
	s.push('x')
	if s.empty() {
		t.Fatal("non-empty stack marked as empty")
	}

	if s.peek() != 'x' {
		t.Fatal("peek of single stack does not return correct value")
	}

	if s.pop() != 'x' {
		t.Fatal("pop of single stack does not return correct value")
	}

	if !s.empty() {
		t.Fatal("popped stack is not empty")
	}
}

func Test_stackMulti(t *testing.T) {
	s := newStack()
	s.push('x')
	s.push('y')
	s.push('z')

	if s.pop() != 'z' {
		t.Fatal("pop of 1st multi stack does not return correct value")
	}
	if s.pop() != 'y' {
		t.Fatal("pop of 2nd multi stack does not return correct value")
	}
	if s.pop() != 'x' {
		t.Fatal("pop of 3rd multi stack does not return correct value")
	}

	if !s.empty() {
		t.Fatal("popped multi stack is not empty")
	}
}
