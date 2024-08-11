package extensions

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_NewStore(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}
	if s == nil {
		t.Fatalf("NewStore() returned nil store")
	}
}

func Test_EmptyStore(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}
	names, err := s.Names()
	if err != nil {
		t.Fatalf("Names() error: %s", err)
	}
	if len(names) != 0 {
		t.Fatalf("Names() returned %d names, expected 0", len(names))
	}

	files, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %s", err)
	}
	if len(files) != 0 {
		t.Fatalf("List() returned %d files, expected 0", len(files))
	}
}

func Test_LoadFromFile(t *testing.T) {
	src := mustTempFile()
	defer os.Remove(src)
	if err := os.WriteFile(src, []byte("test"), 0644); err != nil {
		t.Fatalf("WriteFile() error: %s", err)
	}

	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}

	if err := s.LoadFromFile(src); err != nil {
		t.Fatalf("LoadFromFile() error: %s", err)
	}

	names, err := s.Names()
	if err != nil {
		t.Fatalf("Names() error: %s", err)
	}
	if len(names) != 1 {
		t.Fatalf("Names() returned %d names, expected 1", len(names))
	}
	if names[0] != filepath.Base(src) {
		t.Fatalf("Names() returned %s, expected %s", names[0], filepath.Base(src))
	}

	paths, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %s", err)
	}
	if len(paths) != 1 {
		t.Fatalf("List() returned %d files, expected 1", len(paths))
	}
	if exp, got := s.Dir(), filepath.Dir(paths[0]); exp != got {
		t.Fatalf("List() returned unexpected path %s, expected %s", got, exp)
	}
}

func Test_LoadFromDir(t *testing.T) {
	dir := t.TempDir()
	files := []string{"a", "b", "c", "d"}
	for _, f := range files {
		fpath := dir + "/" + f
		if err := os.WriteFile(fpath, []byte("test"), 0644); err != nil {
			t.Fatalf("WriteFile() error: %s", err)
		}
	}

	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}

	if err := s.LoadFromDir(dir); err != nil {
		t.Fatalf("LoadFromDir() error: %s", err)
	}

	names, err := s.Names()
	if err != nil {
		t.Fatalf("Names() error: %s", err)
	}
	if !stringSliceEqual(names, files) {
		t.Fatalf("Names() returned %v, expected %v", names, files)
	}

	paths, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %s", err)
	}
	if len(paths) != len(files) {
		t.Fatalf("List() returned %d files, expected %d", len(paths), len(files))
	}
	for _, p := range paths {
		if exp, got := s.Dir(), filepath.Dir(p); exp != got {
			t.Fatalf("List() returned unexpected path %s, expected %s", got, exp)
		}
	}
}

func Test_LoadFromEmptyDir(t *testing.T) {
	dir := t.TempDir()

	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}

	if err := s.LoadFromDir(dir); err != nil {
		t.Fatalf("LoadFromDir() error: %s", err)
	}

	names, err := s.Names()
	if err != nil {
		t.Fatalf("Names() error: %s", err)
	}
	if len(names) != 0 {
		t.Fatalf("Names() returned %d names, expected 0", len(names))
	}
	paths, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %s", err)
	}
	if len(paths) != 0 {
		t.Fatalf("List() returned %d files, expected 0", len(paths))
	}
}

func Test_LoadFromZipfile(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}

	if err := s.LoadFromZip("testdata/files-with-dir.zip"); err == nil {
		t.Fatalf("no error when installing a ZIP file with subdirectories")
	}

	if err := s.LoadFromZip("testdata/files.zip"); err != nil {
		t.Fatalf("LoadFromZip() error: %s", err)
	}

	names, err := s.Names()
	if err != nil {
		t.Fatalf("Names() error: %s", err)
	}
	exp := []string{"a", "b", "c", "d"}
	if !stringSliceEqual(names, exp) {
		t.Fatalf("Names() returned %v, expected %v", names, exp)
	}
	paths, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %s", err)
	}
	if len(paths) != len(exp) {
		t.Fatalf("List() returned %d files, expected %d", len(paths), len(exp))
	}
	for _, p := range paths {
		if exp, got := s.Dir(), filepath.Dir(p); exp != got {
			t.Fatalf("List() returned unexpected path %s, expected %s", got, exp)
		}
	}
}

func Test_LoadFromTarGzip(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore() error: %s", err)
	}

	if err := s.LoadFromTarGzip("testdata/files-with-dir.tar.gz"); err == nil {
		t.Fatalf("no error when installing a ZIP file with subdirectories")
	}

	if err := s.LoadFromTarGzip("testdata/files.tar.gz"); err != nil {
		t.Fatalf("LoadFromZip() error: %s", err)
	}

	names, err := s.Names()
	if err != nil {
		t.Fatalf("Names() error: %s", err)
	}
	exp := []string{"a", "b", "c", "d"}
	if !stringSliceEqual(names, exp) {
		t.Fatalf("Names() returned %v, expected %v", names, exp)
	}
	paths, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %s", err)
	}
	if len(paths) != len(exp) {
		t.Fatalf("List() returned %d files, expected %d", len(paths), len(exp))
	}
	for _, p := range paths {
		if exp, got := s.Dir(), filepath.Dir(p); exp != got {
			t.Fatalf("List() returned unexpected path %s, expected %s", got, exp)
		}
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if s != b[i] {
			return false
		}
	}
	return true
}

func mustTempFile() string {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}
