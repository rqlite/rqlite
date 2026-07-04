package plan

import (
	"os"
	"path/filepath"
	"testing"
)

// MockInspector records which Done method LastOpDone dispatched to.
type MockInspector struct {
	Called string
	Ret    bool
	Err    error
}

func (m *MockInspector) RenameDone(src, dst string) (bool, error) {
	m.Called = "rename"
	return m.Ret, m.Err
}
func (m *MockInspector) RemoveDone(path string) (bool, error) {
	m.Called = "remove"
	return m.Ret, m.Err
}
func (m *MockInspector) RemoveAllDone(path string) (bool, error) {
	m.Called = "remove_all"
	return m.Ret, m.Err
}
func (m *MockInspector) CheckpointDone(db string, wals []string) (bool, error) {
	m.Called = "checkpoint"
	return m.Ret, m.Err
}
func (m *MockInspector) WriteMetaDone(dir string, data []byte) (bool, error) {
	m.Called = "write_meta"
	return m.Ret, m.Err
}
func (m *MockInspector) MkdirAllDone(path string) (bool, error) {
	m.Called = "mkdir_all"
	return m.Ret, m.Err
}
func (m *MockInspector) CopyFileDone(src, dst string) (bool, error) {
	m.Called = "copy_file"
	return m.Ret, m.Err
}
func (m *MockInspector) CalcCRC32Done(dataPath, crcPath string) (bool, error) {
	m.Called = "calc_crc32"
	return m.Ret, m.Err
}
func (m *MockInspector) VerifyDBDone(db string) (bool, error) {
	m.Called = "verify_db"
	return m.Ret, m.Err
}

// Test_Plan_LastOpDone_Dispatch verifies that LastOpDone dispatches only the
// plan's final operation, to the matching Inspector method.
func Test_Plan_LastOpDone_Dispatch(t *testing.T) {
	tests := []struct {
		name   string
		build  func(p *Plan)
		expect string
	}{
		{"rename", func(p *Plan) { p.AddRename("a", "b") }, "rename"},
		{"remove", func(p *Plan) { p.AddRemove("a") }, "remove"},
		{"remove_all", func(p *Plan) { p.AddRemoveAll("a") }, "remove_all"},
		{"checkpoint", func(p *Plan) { p.AddCheckpoint("db", []string{"w"}) }, "checkpoint"},
		{"write_meta", func(p *Plan) { p.AddWriteMeta("d", []byte("{}")) }, "write_meta"},
		{"mkdir_all", func(p *Plan) { p.AddMkdirAll("d") }, "mkdir_all"},
		{"copy_file", func(p *Plan) { p.AddCopyFile("s", "d") }, "copy_file"},
		{"calc_crc32", func(p *Plan) { p.AddCalcCRC32("d", "c") }, "calc_crc32"},
		{"verify_db", func(p *Plan) { p.AddVerifyDB("d") }, "verify_db"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			p.AddMkdirAll("leading") // ensure only the LAST op is dispatched
			tt.build(p)

			c := &MockInspector{Ret: true}
			done, err := p.LastOpDone(c)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !done {
				t.Fatal("expected done=true")
			}
			if c.Called != tt.expect {
				t.Fatalf("expected %q dispatched, got %q", tt.expect, c.Called)
			}
		})
	}
}

// Test_Plan_LastOpDone_Empty verifies an empty plan is trivially done and
// dispatches nothing.
func Test_Plan_LastOpDone_Empty(t *testing.T) {
	c := &MockInspector{}
	done, err := New().LastOpDone(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !done {
		t.Fatal("expected empty plan to be done")
	}
	if c.Called != "" {
		t.Fatalf("expected no dispatch for empty plan, got %q", c.Called)
	}
}

// Test_Plan_LastOpDone_DetectsCompletedRename reproduces the reap crash window
// at the plan level: a plan whose terminal rename has already been applied
// cannot be safely replayed (an earlier op references the pre-rename directory),
// but LastOpDone recognizes the plan as already complete.
func Test_Plan_LastOpDone_DetectsCompletedRename(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "snap")
	finalDir := filepath.Join(dir, "final")
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(snapDir, "data.db")
	mustWriteFile(t, dbPath, "dbdata")

	p := New()
	// An earlier op that reads a file inside snapDir...
	p.AddCalcCRC32(dbPath, dbPath+".crc32")
	// ...then the terminal rename that moves snapDir away.
	p.AddRename(snapDir, finalDir)

	if err := p.Execute(NewExecutor()); err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	// Naive replay fails: CalcCRC32 now references the renamed-away path.
	if err := p.Execute(NewExecutor()); err == nil {
		t.Fatal("expected naive replay of a completed plan to fail")
	}

	// LastOpDone recognizes the plan is already complete.
	done, err := p.LastOpDone(NewChecker())
	if err != nil {
		t.Fatalf("LastOpDone error: %v", err)
	}
	if !done {
		t.Fatal("expected LastOpDone to report the plan complete")
	}
}

func Test_Checker_RenameDone(t *testing.T) {
	c := NewChecker()
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	// Neither exists -> not done.
	if done, err := c.RenameDone(src, dst); err != nil || done {
		t.Fatalf("expected not done, got done=%v err=%v", done, err)
	}
	// src present, dst missing -> not done.
	mustWriteFile(t, src, "x")
	if done, err := c.RenameDone(src, dst); err != nil || done {
		t.Fatalf("expected not done with src present, got done=%v err=%v", done, err)
	}
	// src gone, dst present -> done.
	if err := os.Rename(src, dst); err != nil {
		t.Fatal(err)
	}
	if done, err := c.RenameDone(src, dst); err != nil || !done {
		t.Fatalf("expected done after rename, got done=%v err=%v", done, err)
	}
}

func Test_Checker_RemoveDone(t *testing.T) {
	c := NewChecker()
	dir := t.TempDir()
	p := filepath.Join(dir, "f")

	if done, err := c.RemoveDone(p); err != nil || !done {
		t.Fatalf("missing path should be done, got done=%v err=%v", done, err)
	}
	mustWriteFile(t, p, "x")
	if done, err := c.RemoveDone(p); err != nil || done {
		t.Fatalf("present path should be not done, got done=%v err=%v", done, err)
	}
	// RemoveAllDone shares the semantics.
	if done, err := c.RemoveAllDone(p); err != nil || done {
		t.Fatalf("present path should be not done, got done=%v err=%v", done, err)
	}
}

func Test_Checker_CheckpointDone(t *testing.T) {
	c := NewChecker()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "data.db")
	wal := filepath.Join(dir, "x.wal")

	// A source WAL still present -> not done.
	mustWriteFile(t, wal, "w")
	if done, err := c.CheckpointDone(dbPath, []string{wal}); err != nil || done {
		t.Fatalf("expected not done with WAL present, got done=%v err=%v", done, err)
	}
	// WAL consumed, no leftover -wal -> done.
	if err := os.Remove(wal); err != nil {
		t.Fatal(err)
	}
	if done, err := c.CheckpointDone(dbPath, []string{wal}); err != nil || !done {
		t.Fatalf("expected done with WAL consumed, got done=%v err=%v", done, err)
	}
	// A half-applied <db>-wal leftover -> not done.
	mustWriteFile(t, dbPath+"-wal", "leftover")
	if done, err := c.CheckpointDone(dbPath, []string{wal}); err != nil || done {
		t.Fatalf("expected not done with leftover -wal, got done=%v err=%v", done, err)
	}
}

func Test_Checker_WriteMetaDone(t *testing.T) {
	c := NewChecker()
	dir := t.TempDir()
	data := []byte(`{"id":"x"}`)

	// No meta.json -> not done.
	if done, err := c.WriteMetaDone(dir, data); err != nil || done {
		t.Fatalf("expected not done, got done=%v err=%v", done, err)
	}
	// Mismatched content -> not done (existence alone is not enough).
	mustWriteFile(t, filepath.Join(dir, "meta.json"), `{"id":"y"}`)
	if done, err := c.WriteMetaDone(dir, data); err != nil || done {
		t.Fatalf("expected not done for mismatched content, got done=%v err=%v", done, err)
	}
	// Exact content -> done.
	mustWriteFile(t, filepath.Join(dir, "meta.json"), string(data))
	if done, err := c.WriteMetaDone(dir, data); err != nil || !done {
		t.Fatalf("expected done for matching content, got done=%v err=%v", done, err)
	}
}

func Test_Checker_VerifyDBDone(t *testing.T) {
	c := NewChecker()
	// VerifyDB is a read with no persistent effect; completion is never observable.
	if done, err := c.VerifyDBDone("anything"); err != nil || done {
		t.Fatalf("VerifyDBDone should always be (false,nil), got done=%v err=%v", done, err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}
