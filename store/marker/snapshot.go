package marker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Snapshot is a helper to manage snapshot flags on disk.
type Snapshot struct {
	dir string
	mu  sync.Mutex
}

// NewSnapshot returns a new Snapshot.
func NewSnapshot(dir string) *Snapshot {
	return &Snapshot{dir: dir}
}

// Dir returns the directory where the snapshot flags are stored.
func (sf *Snapshot) Dir() string {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.dir
}

// MarkStarted marks the snapshot as started.
func (sf *Snapshot) MarkStarted(term, index uint64) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if ok, _, _, err := sf.isStarted(); err != nil {
		if err != nil {
			return err
		}
	} else if ok {
		return fmt.Errorf("snapshot already STARTED")
	}
	path := filepath.Join(sf.dir, fmt.Sprintf("SNAPSHOT_MARK_STARTED_%d-%d", term, index))
	if err := os.WriteFile(path, nil, 0644); err != nil {
		return err
	}
	return syncFile(path)
}

// ClearStarted removes the started marker.
func (sf *Snapshot) ClearStarted() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if err := removeMatchingFiles(sf.dir, "SNAPSHOT_MARK_STARTED_*"); err != nil {
		return err
	}
	return syncDir(sf.dir)
}

// IsStarted returns true if the snapshot is marked as started.
func (sf *Snapshot) IsStarted() (ok bool, term, index uint64, retErr error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.isStarted()
}

func (sf *Snapshot) isStarted() (ok bool, term, index uint64, retErr error) {
	files, err := filepath.Glob(filepath.Join(sf.dir, "SNAPSHOT_MARK_STARTED_*"))
	if err != nil {
		return false, 0, 0, err
	}
	if len(files) == 0 {
		return false, 0, 0, nil
	}
	if len(files) > 1 {
		return false, 0, 0, fmt.Errorf("multiple STARTED markers found")
	}

	_, err = fmt.Sscanf(filepath.Base(files[0]), "SNAPSHOT_MARK_STARTED_%d-%d", &term, &index)
	if err != nil {
		return false, 0, 0, err
	}
	return true, term, index, nil
}

// MarkCheckpointed marks the snapshot as checkpointed.
func (sf *Snapshot) MarkCheckpointed(term, index uint64) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if ok, _, _, err := sf.isCheckpointed(); err != nil {
		if err != nil {
			return err
		}
	} else if ok {
		return fmt.Errorf("snapshot already CHECKPOINTED")
	}
	path := filepath.Join(sf.dir, fmt.Sprintf("SNAPSHOT_MARK_CHECKPOINTED_%d-%d", term, index))
	if err := os.WriteFile(path, nil, 0644); err != nil {
		return err
	}
	return syncFile(path)
}

// ClearCheckpointed removes the checkpointed marker.
func (sf *Snapshot) ClearCheckpointed() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if err := removeMatchingFiles(sf.dir, "SNAPSHOT_MARK_CHECKPOINTED_*"); err != nil {
		return err
	}
	return syncDir(sf.dir)
}

// IsCheckpointed returns true if the snapshot is marked as checkpointed.
func (sf *Snapshot) IsCheckpointed() (ok bool, term, index uint64, retErr error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	return sf.isCheckpointed()
}

func (sf *Snapshot) isCheckpointed() (ok bool, term, index uint64, retErr error) {
	files, err := filepath.Glob(filepath.Join(sf.dir, "SNAPSHOT_MARK_CHECKPOINTED_*"))
	if err != nil {
		return false, 0, 0, err
	}
	if len(files) == 0 {
		return false, 0, 0, nil
	}
	if len(files) > 1 {
		return false, 0, 0, fmt.Errorf("multiple CHECKPOINTED markers found")
	}

	_, err = fmt.Sscanf(filepath.Base(files[0]), "SNAPSHOT_MARK_CHECKPOINTED_%d-%d", &term, &index)
	if err != nil {
		return false, 0, 0, err
	}
	return true, term, index, nil
}

// Clear removes all snapshot flags.
func (sf *Snapshot) Clear() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if err := removeMatchingFiles(sf.dir, "SNAPSHOT_MARK_STARTED_*"); err != nil {
		return err
	}
	if err := removeMatchingFiles(sf.dir, "SNAPSHOT_MARK_CHECKPOINTED_*"); err != nil {
		return err
	}
	return syncDir(sf.dir)
}

func removeMatchingFiles(dir, prefix string) error {
	files, err := filepath.Glob(filepath.Join(dir, prefix))
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return err
		}
	}
	return nil
}

func syncFile(path string) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()
	return fh.Sync()
}
