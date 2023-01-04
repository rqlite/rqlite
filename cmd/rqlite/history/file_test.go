package history

import (
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

	Delete()
	if exists(p) {
		t.Fatal("history file exists after deletion")
	}
}

func exists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
