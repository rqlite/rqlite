package main

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForbiddenUses(t *testing.T) {
	tests := []struct {
		name string
		path string
		src  string
		want int
	}{
		{"calls", "app.go", `package p; import "os"; func f() { os.Rename("a", "b"); os.Remove("a"); os.RemoveAll("a") }`, 3},
		{"alias", "app.go", `package p; import stdlibos "os"; func f() { stdlibos.Remove("a") }`, 1},
		{"function references", "app.go", `package p; import "os"; var remove = os.Remove; func f() { rename := os.Rename; _ = rename }`, 2},
		{"dot import", "app.go", `package p; import . "os"; func f() { Remove("a"); defer Rename("a", "b"); go RemoveAll("a") }`, 3},
		{"dot import reference", "app.go", `package p; import . "os"; var remove = Remove`, 1},
		{"comments and strings", "app.go", "package p\n// os.Remove(\"a\")\nvar s = `os.Rename(\"a\", \"b\")`", 0},
		{"other operations", "app.go", `package p; import "os"; func f() { os.Stat("a"); os.MkdirAll("a", 0755) }`, 0},
		{"other package", "app.go", `package p; import os "example.com/other"; func f() { os.Remove("a") }`, 0},
		{"shadowed import", "app.go", `package p; import "os"; func f() { os := struct{ Remove func(string) }{}; os.Remove("a") }`, 0},
		{"shadowed alias parameter", "app.go", `package p; import stdlibos "os"; func f(stdlibos interface{ Remove(string) }) { stdlibos.Remove("a") }`, 0},
		{"shadowed dot import", "app.go", `package p; import . "os"; func f() { Remove := func(string) {}; Remove("a") }`, 0},
		{"dot import unrelated selector", "app.go", `package p; import . "os"; func f(other interface{ Remove(string) }) { other.Remove("a") }`, 0},
		{"wrappers", "internal/fsutil/fsutil.go", `package fsutil; import "os"; func Rename(a, b string) error { return os.Rename(a,b) }; func Remove(a string) error { return os.Remove(a) }; func RemoveAll(a string) error { return os.RemoveAll(a) }`, 0},
		{"retry helpers", "internal/fsutil/fsutil.go", `package fsutil; import "os"; func RenameWithRetry() { f := func() { os.Rename("a", "b") }; _ = f }; func RemoveWithRetry() { os.Remove("a") }; func RemoveAllWithRetry() { os.RemoveAll("a") }`, 0},
		{"wrong wrapper operation", "internal/fsutil/fsutil.go", `package fsutil; import "os"; func Rename() { os.Remove("a") }`, 1},
		{"other helper", "internal/fsutil/fsutil.go", `package fsutil; import "os"; func cleanup() { os.Remove("a") }`, 1},
		{"package initializer", "internal/fsutil/fsutil.go", `package fsutil; import "os"; var remove = os.Remove`, 1},
		{"other fsutil file", "internal/fsutil/copy.go", `package fsutil; import "os"; func Remove() { os.Remove("a") }`, 1},
		{"other package wrapper", "app.go", `package p; import "os"; func Remove() { os.Remove("a") }`, 1},
		{"method", "internal/fsutil/fsutil.go", `package fsutil; import "os"; type T struct{}; func (T) Remove() { os.Remove("a") }`, 1},
		{"wrong package name", "internal/fsutil/fsutil.go", `package p; import "os"; func Remove() { os.Remove("a") }`, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), tt.path, tt.src, parser.AllErrors)
			if err != nil {
				t.Fatal(err)
			}
			if got := forbiddenUses(file, tt.path); len(got) != tt.want {
				t.Fatalf("got %d violations, want %d: %v", len(got), tt.want, got)
			}
		})
	}
}

func TestCheck(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod":                    "module example.com/test\n",
		"app_test.go":               "package p\nimport \"os\"\nfunc f() { os.Remove(\"a\") }\n",
		"platform_windows.go":       "//go:build windows\n\npackage p\nimport \"os\"\nfunc g() { os.Rename(\"a\", \"b\") }\n",
		"testdata/sample.go":        "package p; import \"os\"; var remove = os.RemoveAll",
		"internal/fsutil/fsutil.go": "package fsutil; import \"os\"; func RemoveAll(p string) error { return os.RemoveAll(p) }",
		"vendor/example/bad.go":     "not Go source",
		".git/bad.go":               "not Go source",
		"README.md":                 "os.Remove(\"a\")",
	}
	for name, data := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	n, err := check(root, &out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("got %d violations, want 3: %s", n, out.String())
	}
	for _, want := range []string{
		"app_test.go:3:12: use fsutil.Remove instead of os.Remove",
		"platform_windows.go:5:12: use fsutil.Rename instead of os.Rename",
		"testdata/sample.go:1:38: use fsutil.RemoveAll instead of os.RemoveAll",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing diagnostic %q in %s", want, out.String())
		}
	}
}

func TestCheckErrors(t *testing.T) {
	root := t.TempDir()
	var out bytes.Buffer
	if _, err := check(root, &out); err == nil {
		t.Fatal("expected an error for a root without go.mod")
	}
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/test\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bad.go"), []byte("not Go source"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := check(root, &out); err == nil {
		t.Fatal("expected an error for invalid Go source")
	}
}
