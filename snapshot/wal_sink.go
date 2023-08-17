package snapshot

// import (
// 	"encoding/json"
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"runtime"
// )

// // walSnapshotSink is a sink for a snapshot.
// type walSnapshotSink struct {
// 	store *WALSnapshotStore

// 	dir       string   // The directory to store the snapshot in.
// 	parentDir string   // The parent directory of the snapshot.
// 	dataFd    *os.File // The file to write the snapshot data to.

// 	meta *walSnapshotMeta

// 	logger *log.Logger
// 	closed bool
// }

// // Write writes the given bytes to the snapshot.
// func (w *walSnapshotSink) Write(p []byte) (n int, err error) {
// 	return w.dataFd.Write(p)
// }

// // Cancel closes the snapshot and removes it.
// func (w *walSnapshotSink) Cancel() error {
// 	w.closed = true
// 	return w.cleanup()
// }

// // ID returns the ID of the snapshot.
// func (w *walSnapshotSink) ID() string {
// 	return w.meta.ID
// }

// func (w *walSnapshotSink) cleanup() error {
// 	w.dataFd.Close()
// 	os.Remove(w.dataFd.Name())
// 	os.Remove(nonTmpName(w.dataFd.Name()))
// 	os.RemoveAll(w.dir)
// 	os.RemoveAll(nonTmpName(w.dir))
// 	return nil
// }

// func (w *walSnapshotSink) writeMeta(full bool) error {
// 	fh, err := os.Create(filepath.Join(w.dir, metaFileName))
// 	if err != nil {
// 		return err
// 	}
// 	defer fh.Close()

// 	w.meta.Full = full

// 	// Write out as JSON
// 	enc := json.NewEncoder(fh)
// 	if err = enc.Encode(w.meta); err != nil {
// 		return err
// 	}

// 	if err := fh.Sync(); err != nil {
// 		return err
// 	}
// 	return fh.Close()
// }

// // WALFullSnapshotSink is a sink for a full snapshot.
// type WALFullSnapshotSink struct {
// 	walSnapshotSink
// }

// // Close closes the snapshot.
// func (w *WALFullSnapshotSink) Close() (retErr error) {
// 	if w.closed {
// 		return nil
// 	}
// 	w.closed = true

// 	defer func() {
// 		if retErr != nil {
// 			w.cleanup()
// 		}
// 	}()

// 	if err := w.dataFd.Sync(); err != nil {
// 		w.logger.Printf("failed syncing snapshot SQLite file: %s", err)
// 		return err
// 	}

// 	if err := w.dataFd.Close(); err != nil {
// 		w.logger.Printf("failed closing snapshot SQLite file: %s", err)
// 		return err
// 	}

// 	if err := w.writeMeta(true); err != nil {
// 		return err
// 	}

// 	if _, err := moveFromTmp(w.dataFd.Name()); err != nil {
// 		w.logger.Printf("failed to move SQLite file into place: %s", err)
// 		return err
// 	}

// 	dstDir, err := moveFromTmp(w.dir)
// 	if err != nil {
// 		w.logger.Printf("failed to move snapshot directory into place: %s", err)
// 		return err
// 	}

// 	// Sync parent directory to ensure snapshot is visible, but it's only
// 	// needed on *nix style file systems.
// 	if runtime.GOOS != "windows" {
// 		if err := syncDir(w.parentDir); err != nil {
// 			w.logger.Printf("failed syncing parent directory: %s", err)
// 			return err
// 		}
// 	}

// 	w.logger.Printf("full snapshot (ID %s) written to %s", w.meta.ID, dstDir)
// 	return nil
// }

// // WALIncrementalSnapshotSink is a sink for an incremental snapshot.
// type WALIncrementalSnapshotSink struct {
// 	walSnapshotSink
// }

// // Close closes the snapshot.
// func (w *WALIncrementalSnapshotSink) Close() (retErr error) {
// 	if w.closed {
// 		return nil
// 	}
// 	w.closed = true

// 	defer func() {
// 		if retErr != nil {
// 			w.cleanup()
// 		}
// 	}()

// 	if err := w.dataFd.Sync(); err != nil {
// 		w.logger.Printf("failed syncing snapshot SQLite file: %s", err)
// 		return err
// 	}

// 	if err := w.dataFd.Close(); err != nil {
// 		w.logger.Printf("failed closing snapshot SQLite file: %s", err)
// 		return err
// 	}

// 	if err := w.writeMeta(false); err != nil {
// 		return err
// 	}

// 	dstDir, err := moveFromTmp(w.dir)
// 	if err != nil {
// 		w.logger.Printf("failed to move snapshot directory into place: %s", err)
// 		return err
// 	}

// 	// Sync parent directory to ensure snapshot is visible, but it's only
// 	// needed on *nix style file systems.
// 	if runtime.GOOS != "windows" {
// 		if err := syncDir(w.parentDir); err != nil {
// 			w.logger.Printf("failed syncing parent directory: %s", err)
// 			return err
// 		}
// 	}
// 	w.logger.Printf("incremental snapshot (ID %s) written to %s", w.meta.ID, dstDir)
// 	return nil
// }
