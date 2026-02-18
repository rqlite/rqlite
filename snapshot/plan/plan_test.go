package plan

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
	p.AddWriteMeta("dir", []byte(`{"id":"test"}`))
	p.AddMkdirAll("newdir")
	p.AddCopyFile("src", "dst")

	if p.Len() != 7 {
		t.Fatalf("expected length 7, got %d", p.Len())
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
	if ops[4].Type != OpWriteMeta || ops[4].Dst != "dir" || string(ops[4].Data) != `{"id":"test"}` {
		t.Errorf("unexpected op 4: %+v", ops[4])
	}
	if ops[5].Type != OpMkdirAll || ops[5].Dst != "newdir" {
		t.Errorf("unexpected op 5: %+v", ops[5])
	}
	if ops[6].Type != OpCopyFile || ops[6].Src != "src" || ops[6].Dst != "dst" {
		t.Errorf("unexpected op 6: %+v", ops[6])
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

func (m *MockVisitor) Checkpoint(db string, wals []string) (int, error) {
	m.Calls = append(m.Calls, "checkpoint "+db)
	return 0, m.Err
}

func (m *MockVisitor) WriteMeta(dir string, data []byte) error {
	m.Calls = append(m.Calls, "write_meta "+dir)
	return m.Err
}

func (m *MockVisitor) MkdirAll(path string) error {
	m.Calls = append(m.Calls, "mkdir_all "+path)
	return m.Err
}

func (m *MockVisitor) CopyFile(src, dst string) error {
	m.Calls = append(m.Calls, "copy_file "+src+"->"+dst)
	return m.Err
}

func TestExecute_Success(t *testing.T) {
	p := New()
	p.AddRename("src", "dst")
	p.AddRemove("file")
	p.AddWriteMeta("snap", []byte(`{}`))

	v := &MockVisitor{}
	if err := p.Execute(v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(v.Calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(v.Calls))
	}
	if v.Calls[0] != "rename src->dst" {
		t.Errorf("unexpected call 0: %s", v.Calls[0])
	}
	if v.Calls[1] != "remove file" {
		t.Errorf("unexpected call 1: %s", v.Calls[1])
	}
	if v.Calls[2] != "write_meta snap" {
		t.Errorf("unexpected call 2: %s", v.Calls[2])
	}
}

func TestExecute_MkdirAllAndCopyFile(t *testing.T) {
	p := New()
	tmpDirPath := filepath.Join(t.TempDir(), "plan_test_dir")
	p.AddMkdirAll(tmpDirPath)
	p.AddCopyFile("src", "dst")

	v := &MockVisitor{}
	if err := p.Execute(v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(v.Calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(v.Calls))
	}
	if v.Calls[0] != "mkdir_all "+tmpDirPath {
		t.Errorf("unexpected call 0: %s", v.Calls[0])
	}
	if v.Calls[1] != "copy_file src->dst" {
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

func (f *FailVisitor) Rename(src, dst string) error                     { return f.check() }
func (f *FailVisitor) Remove(path string) error                         { return f.check() }
func (f *FailVisitor) RemoveAll(path string) error                      { return f.check() }
func (f *FailVisitor) Checkpoint(db string, wals []string) (int, error) { return 0, f.check() }
func (f *FailVisitor) WriteMeta(dir string, data []byte) error          { return f.check() }
func (f *FailVisitor) MkdirAll(path string) error                       { return f.check() }
func (f *FailVisitor) CopyFile(src, dst string) error                   { return f.check() }

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
	p.AddWriteMeta("snap", []byte(`{"id":"snap-1"}`))
	p.AddMkdirAll("newdir")
	p.AddCopyFile("x", "y")
	p.AddRemoveAll("f")
	p.NReaped = 3
	p.NCheckpointed = 2

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
	if p2.NReaped != 3 {
		t.Fatalf("expected NReaped=3, got %d", p2.NReaped)
	}
	if p2.NCheckpointed != 2 {
		t.Fatalf("expected NCheckpointed=2, got %d", p2.NCheckpointed)
	}
}

func TestFilePersistence(t *testing.T) {
	p := New()
	p.AddRename("a", "b")
	p.AddRemove("c")
	p.AddCheckpoint("db", []string{"w1"})
	p.AddWriteMeta("snap", []byte(`{"id":"snap-1"}`))
	p.NReaped = 1
	p.NCheckpointed = 1

	tmpFile, err := os.CreateTemp("", "plan_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	if err := WriteToFile(p, tmpFile.Name()); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}

	p2, err := ReadFromFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("ReadFromFile failed: %v", err)
	}

	if !reflect.DeepEqual(p.Ops, p2.Ops) {
		t.Fatalf("plans do not match after file roundtrip.\nOriginal: %+v\nRead: %+v", p.Ops, p2.Ops)
	}
	if p2.NReaped != 1 || p2.NCheckpointed != 1 {
		t.Fatalf("expected NReaped=1, NCheckpointed=1, got %d, %d", p2.NReaped, p2.NCheckpointed)
	}
}
