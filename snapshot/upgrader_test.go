package snapshot

import (
	"log"
	"os"
	"testing"
)

func Test_Upgrade_NothingToDo(t *testing.T) {
	logger := log.New(os.Stderr, "[snapshot-store-upgrader] ", 0)
	if err := Upgrade("/does/not/exist", "/does/not/exist/either", logger); err != nil {
		t.Fatalf("failed to upgrade non-existent directories: %s", err)
	}

	oldEmpty := t.TempDir()
	newEmpty := t.TempDir()
	if err := Upgrade(oldEmpty, newEmpty, logger); err != nil {
		t.Fatalf("failed to upgrade empty directories: %s", err)
	}
}
