package history

import (
	"errors"
	"io/fs"
	"os"
	"testing"
)

func Test_Delete(t *testing.T) {
	w := Writer()
	if w == nil {
		t.Fatal("failed to create history writer")
	}

	p, err := Path()
	if err != nil {
		t.Fatalf("failed to get history file path: %s", err.Error())
	}

	if !exists(p) {
		t.Fatal("history file does not exist")
	}

	w.Close()
	if err := Delete(); err != nil {
		t.Fatalf("failed to delete history file: %s", err.Error())
	}
	if exists(p) {
		t.Fatal("history file exists after deletion")
	}
}

func exists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return !errors.Is(err, fs.ErrNotExist)
	}
	return true
}
