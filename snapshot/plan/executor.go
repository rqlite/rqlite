package plan

import (
	"os"
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

// Checkpoint is a no-op placeholder.
func (e *Executor) Checkpoint(db string, wals []string) error {
	return nil
}
