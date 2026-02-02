package plan

import (
	"os"

	"github.com/rqlite/rqlite/v9/db"
)

// Executor implements the Visitor interface to execute filesystem operations.
type Executor struct{}

// NewExecutor returns a new Executor.
func NewExecutor() *Executor {
	return &Executor{}
}

// Rename renames a file. It is idempotent: if src does not exist but dst does,
// it returns nil.
func (e *Executor) Rename(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		// If source does not exist, check if destination exists.
		if _, statErr := os.Stat(dst); statErr == nil {
			return nil
		}
	}
	return err
}

// Remove removes a file. It is idempotent: if the file does not exist, it returns nil.
func (e *Executor) Remove(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// RemoveAll removes a directory and any children. It is idempotent.
func (e *Executor) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// Checkpoint performs a WAL checkpoint the given WAL files into the database
// at dbPath.
func (e *Executor) Checkpoint(dbPath string, wals []string) error {
	existingWals := []string{}
	for _, wal := range wals {
		if _, err := os.Stat(wal); err == nil {
			existingWals = append(existingWals, wal)
		}
	}

	if len(existingWals) == 0 {
		return nil
	}
	return db.ReplayWAL(dbPath, existingWals, false)
}
