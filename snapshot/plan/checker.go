package plan

import (
	"bytes"
	"os"
	"path/filepath"
)

// Checker reports whether a plan's operations have already been applied on disk.
// It is the read-only counterpart to Executor: Executor performs operations,
// Checker inspects whether they are done.
//
// The Done methods perform no mutations. Each corresponds to the like-named
// mutating Executor method.
type Checker struct{}

// NewChecker returns a new Checker.
func NewChecker() *Checker {
	return &Checker{}
}

// Confirm at compile-time that Check implements Inspector.
var _ Inspector = (*Checker)(nil)

// RenameDone reports whether a rename has taken effect: the source is gone and
// the destination is present. Because os.Rename is atomic there is no
// partially-applied state to consider.
func (c *Checker) RenameDone(src, dst string) (bool, error) {
	srcExists, err := pathExists(src)
	if err != nil {
		return false, err
	}
	if srcExists {
		return false, nil
	}
	return pathExists(dst)
}

// RemoveDone reports whether the path is gone.
func (c *Checker) RemoveDone(path string) (bool, error) {
	exists, err := pathExists(path)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

// RemoveAllDone reports whether the path is gone.
func (c *Checker) RemoveAllDone(path string) (bool, error) {
	return c.RemoveDone(path)
}

// CheckpointDone reports whether every source WAL has been consumed and no
// half-applied <db>-wal remains -- the state Checkpoint leaves behind once all
// WALs have been checkpointed into the database.
func (c *Checker) CheckpointDone(dbPath string, wals []string) (bool, error) {
	leftover, err := pathExists(dbPath + "-wal")
	if err != nil {
		return false, err
	}
	if leftover {
		return false, nil
	}
	for _, w := range wals {
		exists, err := pathExists(w)
		if err != nil {
			return false, err
		}
		if exists {
			return false, nil
		}
	}
	return true, nil
}

// WriteMetaDone reports whether meta.json in dir already holds exactly data.
// Content is compared, not merely existence, because a snapshot directory
// generally already contains a meta.json from before the write.
func (c *Checker) WriteMetaDone(dir string, data []byte) (bool, error) {
	got, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(got, data), nil
}

// MkdirAllDone reports whether the directory exists.
func (c *Checker) MkdirAllDone(path string) (bool, error) {
	return pathExists(path)
}

// CopyFileDone reports whether the destination exists. CopyFile is not atomic,
// so a destination left partially written by an interrupted copy is reported as
// done; a plan relying on copy as its final operation must account for that.
func (c *Checker) CopyFileDone(src, dst string) (bool, error) {
	srcFi, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return pathExists(dst)
		}
		return false, err
	}
	dstFi, err := os.Stat(dst)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return srcFi.Size() == dstFi.Size(), nil
}

// CalcCRC32Done reports whether the sidecar file exists. It does not recompute
// and compare the checksum, so a stale sidecar is reported as done.
func (c *Checker) CalcCRC32Done(dataPath, crcPath string) (bool, error) {
	return pathExists(crcPath)
}

// VerifyDBDone always reports false. VerifyDB is a read-only integrity check
// with no persistent effect, so its completion cannot be observed from disk.
// Reporting false makes a caller fall back to re-running the harmless check
// rather than incorrectly concluding a plan is complete.
func (c *Checker) VerifyDBDone(path string) (bool, error) {
	return false, nil
}

// pathExists reports whether a filesystem path exists. Non-existence is
// reported as (false, nil); any other stat error is returned.
func pathExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
