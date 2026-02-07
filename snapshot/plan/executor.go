package plan

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/db"
)

// Executor implements the Visitor interface to execute snapshot store operations.
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

// Checkpoint performs a WAL checkpoint of the given WAL files into the
// database at dbPath. WAL files may reside in any directory; each is
// renamed into the database's directory before checkpointing.
//
// If any WAL file does not exist, it is skipped. If no WAL files
// exist, no checkpoint is attempted. The number of checkpointed
// WAL files is returned.
//
// If any WAL files do exist, but the dbPath does not, an error is
// returned.
//
// Checkpoint is idempotent: if the process crashes after a WAL has
// been renamed into the checkpoint position but before the checkpoint
// completes, a subsequent call will detect and finish the incomplete
// checkpoint before processing the remaining WALs.
func (e *Executor) Checkpoint(dbPath string, wals []string) (int, error) {
	walPath := dbPath + "-wal"

	// Handle a leftover WAL from a previous interrupted checkpoint.
	if _, err := os.Stat(walPath); err == nil {
		if err := db.CheckpointRemove(dbPath); err != nil {
			return 0, fmt.Errorf("checkpoint leftover WAL: %w", err)
		}
	}

	existingWals := []string{}
	for _, wal := range wals {
		if _, err := os.Stat(wal); err == nil {
			existingWals = append(existingWals, wal)
		}
	}
	n := len(existingWals)
	if n == 0 {
		return 0, nil
	}

	if _, err := os.Stat(dbPath); err != nil {
		return 0, err
	}

	for _, wal := range existingWals {
		if err := os.Rename(wal, walPath); err != nil {
			return 0, fmt.Errorf("moving WAL %s: %w", wal, err)
		}
		if err := db.CheckpointRemove(dbPath); err != nil {
			return 0, fmt.Errorf("checkpointing WAL: %w", err)
		}
	}
	return n, nil
}

// WriteMeta writes data to a meta.json file in the given directory. It is
// idempotent: if the directory no longer exists (because a subsequent rename
// in the plan already moved it), it returns nil.
func (e *Executor) WriteMeta(dir string, data []byte) error {
	path := filepath.Join(dir, "meta.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}
