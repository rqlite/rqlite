package plan

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestNewPlan(t *testing.T) {
	p := New()
	if p == nil {
		t.Fatal("New() returned nil")
	}
	if p.Len() != 0 {
		t.Fatalf("expected length 0, got %d", p.Len())
	}
}

func TestAddOperations(t *testing.T) {
	p := New()
	p.AddRename("a", "b")
	p.AddRemove("c")
	p.AddRemoveAll("d")
	p.AddCheckpoint("db", []string{"w1", "w2"})

	if p.Len() != 4 {
		t.Fatalf("expected length 4, got %d", p.Len())
	}

	ops := p.Ops
	if ops[0].Type != OpRename || ops[0].Src != "a" || ops[0].Dst != "b" {
		t.Errorf("unexpected op 0: %+v", ops[0])
	}
	if ops[1].Type != OpRemove || ops[1].Src != "c" {
		t.Errorf("unexpected op 1: %+v", ops[1])
	}
	if ops[2].Type != OpRemoveAll || ops[2].Src != "d" {
		t.Errorf("unexpected op 2: %+v", ops[2])
	}
	if ops[3].Type != OpCheckpoint || ops[3].DB != "db" || len(ops[3].WALs) != 2 {
		t.Errorf("unexpected op 3: %+v", ops[3])
	}
}

// MockVisitor records calls for verification
type MockVisitor struct {
	Calls []string
	Err   error
}

func (m *MockVisitor) Rename(src, dst string) error {
	m.Calls = append(m.Calls, "rename "+src+"->"+dst)
	return m.Err
}

func (m *MockVisitor) Remove(path string) error {
	m.Calls = append(m.Calls, "remove "+path)
	return m.Err
}

func (m *MockVisitor) RemoveAll(path string) error {
	m.Calls = append(m.Calls, "remove_all "+path)
	return m.Err
}

func (m *MockVisitor) Checkpoint(db string, wals []string) error {
	m.Calls = append(m.Calls, "checkpoint "+db)
	return m.Err
}

func TestExecute_Success(t *testing.T) {
	p := New()
	p.AddRename("src", "dst")
	p.AddRemove("file")

	v := &MockVisitor{}
	if err := p.Execute(v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(v.Calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(v.Calls))
	}
	if v.Calls[0] != "rename src->dst" {
		t.Errorf("unexpected call 0: %s", v.Calls[0])
	}
	if v.Calls[1] != "remove file" {
		t.Errorf("unexpected call 1: %s", v.Calls[1])
	}
}

func TestExecute_Error(t *testing.T) {
	p := New()
	p.AddRemove("first")

	v := &MockVisitor{Err: errors.New("visitor error")}
	err := p.Execute(v)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if exp, got := "visitor error", err.Error(); exp != got {
		t.Fatalf(`expected "%s", got %s`, exp, got)
	}
}

type FailVisitor struct {
	Count  int
	FailAt int
}

func (f *FailVisitor) check() error {
	f.Count++
	if f.Count == f.FailAt {
		return errors.New("fail")
	}
	return nil
}

func (f *FailVisitor) Rename(src, dst string) error              { return f.check() }
func (f *FailVisitor) Remove(path string) error                  { return f.check() }
func (f *FailVisitor) RemoveAll(path string) error               { return f.check() }
func (f *FailVisitor) Checkpoint(db string, wals []string) error { return f.check() }

func TestExecute_StopsOnError(t *testing.T) {
	p := New()
	p.AddRename("1", "2")
	p.AddRemove("3")
	p.AddRemove("4")

	v := &FailVisitor{FailAt: 2} // Should fail on second op
	err := p.Execute(v)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "fail" {
		t.Fatalf("expected 'fail' error, got %v", err)
	}
	if v.Count != 2 {
		t.Fatalf("expected 2 calls (fail on 2nd), got %d", v.Count)
	}
}

func TestExecute_Empty(t *testing.T) {
	p := New()
	v := &MockVisitor{}
	if err := p.Execute(v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v.Calls) != 0 {
		t.Errorf("expected 0 calls, got %d", len(v.Calls))
	}
}

func TestJSONSerialization(t *testing.T) {
	p := New()
	p.AddRename("a", "b")
	p.AddRename("b", "c")
	p.AddRemove("d")
	p.AddRemoveAll("e")
	p.AddCheckpoint("db", []string{"w1", "w2"})
	p.AddRemoveAll("f")

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	p2 := New()
	if err := json.Unmarshal(data, p2); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !reflect.DeepEqual(p.Ops, p2.Ops) {
		t.Fatalf("plans do not match.\nOriginal: %+v\nDecoded: %+v", p.Ops, p2.Ops)
	}
}

func TestFilePersistence(t *testing.T) {
	p := New()
	p.AddRename("a", "b")
	p.AddRemove("c")
	p.AddCheckpoint("db", []string{"w1"})

	tmpFile, err := os.CreateTemp("", "plan_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	if err := p.WriteToFile(tmpFile.Name()); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	p2, err := ReadFromFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("ReadFromFile failed: %v", err)
	}

	if !reflect.DeepEqual(p.Ops, p2.Ops) {
		t.Fatalf("plans do not match after file roundtrip.\nOriginal: %+v\nRead: %+v", p.Ops, p2.Ops)
	}
}
