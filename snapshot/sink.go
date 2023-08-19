package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rqlite/rqlite/db"
)

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	workDir    string
	curGenDir  string
	nextGenDir string
	meta       *Meta

	dataFD *os.File

	logger *log.Logger
	closed bool
}

// NewSink creates a new Sink object.
func NewSink(workDir, currGenDir, nextGenDir string, meta *Meta) *Sink {
	return &Sink{
		workDir:    workDir,
		curGenDir:  currGenDir,
		nextGenDir: nextGenDir,
		meta:       meta,
		logger:     log.New(os.Stderr, "snapshot-sink: ", log.LstdFlags),
	}
}

// Open opens the sink for writing.
func (s *Sink) Open() error {
	dataPath := filepath.Join(s.workDir, "snapshot-data.tmp")
	dataFD, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	s.dataFD = dataFD
	return nil
}

// Write writes snapshot data to the sink.
func (s *Sink) Write(p []byte) (n int, err error) {
	return s.dataFD.Write(p)
}

// ID returns the ID of the snapshot being written.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Cancel cancels the snapshot.
func (s *Sink) Cancel() error {
	s.closed = true
	return s.cleanup()
}

// Close closes the sink, and finalizes creation of the snapshot.
func (s *Sink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.cleanup()
	return s.processSnapshotData()
}

func (s *Sink) processSnapshotData() error {
	if _, err := s.dataFD.Seek(0, 0); err != nil {
		return err
	}

	strHdr, _, err := NewStreamHeaderFromReader(s.dataFD)
	if err != nil {
		return fmt.Errorf("error reading stream header: %v", err)
	}
	if strHdr.GetVersion() != streamVersion {
		return fmt.Errorf("unsupported snapshot version %d", strHdr.GetVersion())
	}

	if incSnap := strHdr.GetIncrementalSnapshot(); incSnap != nil {
		return s.processIncrementalSnapshot(incSnap)
	}
	fullSnap := strHdr.GetFullSnapshot()
	if fullSnap == nil {
		return fmt.Errorf("got nil FullSnapshot")
	}
	return s.processFullSnapshot(fullSnap)
}

func (s *Sink) processIncrementalSnapshot(incSnap *IncrementalSnapshot) error {
	s.logger.Printf("processing incremental snapshot")

	incSnapDir := tmpName(filepath.Join(s.curGenDir, s.meta.ID))
	walPath := filepath.Join(incSnapDir, snapWALFile)
	if err := os.WriteFile(walPath, incSnap.Data, 0644); err != nil {
		return fmt.Errorf("error writing WAL data: %v", err)
	}
	if err := s.writeMeta(incSnapDir, false); err != nil {
		return err
	}

	// We're done! Move the directory into place.
	dstDir, err := moveFromTmpSync(incSnapDir)
	if err != nil {
		s.logger.Printf("failed to move incremental snapshot directory into place: %s", err)
		return err
	}
	s.logger.Printf("incremental snapshot (ID %s) written to %s", s.meta.ID, dstDir)
	return nil
}

func (s *Sink) processFullSnapshot(fullSnap *FullSnapshot) error {
	s.logger.Printf("processing full snapshot")

	// We need a new generational directory, and need to create the first
	// snapshot in that directory.
	nextGenDir := tmpName(s.nextGenDir)
	if err := os.MkdirAll(nextGenDir, 0755); err != nil {
		return fmt.Errorf("error creating full snapshot directory: %v", err)
	}

	// Write out base SQLite file.
	sqliteBasePath := filepath.Join(nextGenDir, baseSqliteFile)
	dbInfo := fullSnap.GetDb()
	if dbInfo == nil {
		return fmt.Errorf("got nil DB info")
	}
	sqliteBaseFD, err := os.Create(sqliteBasePath)
	if err != nil {
		return fmt.Errorf("error creating SQLite file: %v", err)
	}
	if _, err := io.CopyN(sqliteBaseFD, s.dataFD, dbInfo.Size); err != nil {
		return fmt.Errorf("error writing SQLite file data: %v", err)
	}
	sqliteBaseFD.Close()

	// Write out any WALs.
	var walFiles []string
	for i, wal := range fullSnap.GetWals() {
		if wal == nil {
			return fmt.Errorf("got nil WAL")
		}

		walName := filepath.Join(nextGenDir, baseSqliteWALFile+fmt.Sprintf("%d", i))
		walFD, err := os.Create(walName)
		if err != nil {
			return fmt.Errorf("error creating WAL file: %v", err)
		}
		if _, err := io.CopyN(walFD, s.dataFD, wal.Size); err != nil {
			return fmt.Errorf("error writing WAL file data: %v", err)
		}
		walFD.Close()
		walFiles = append(walFiles, walName)
	}

	// Checkpoint the WAL files into the base SQLite file
	if err := db.ReplayWAL(sqliteBasePath, walFiles, false); err != nil {
		return fmt.Errorf("error checkpointing WAL: %v", err)
	}

	// Now create the first snapshot directory in the new generation.
	snapDir := filepath.Join(nextGenDir, s.meta.ID)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return fmt.Errorf("error creating full snapshot directory: %v", err)
	}
	if err := s.writeMeta(snapDir, true); err != nil {
		return err
	}

	// We're done! Move the directory into place.
	dstDir, err := moveFromTmpSync(nextGenDir)
	if err != nil {
		s.logger.Printf("failed to move full snapshot directory into place: %s", err)
		return err
	}
	s.logger.Printf("full snapshot (ID %s) written to %s", s.meta.ID, dstDir)
	return nil
}

func (s *Sink) writeMeta(dir string, full bool) error {
	fh, err := os.Create(filepath.Join(dir, metaFileName))
	if err != nil {
		return err
	}
	defer fh.Close()
	s.meta.Full = full

	// Write out as JSON
	enc := json.NewEncoder(fh)
	if err = enc.Encode(s.meta); err != nil {
		return err
	}

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}

func (s *Sink) cleanup() error {
	if s.dataFD != nil {
		if err := s.dataFD.Close(); err != nil {
			return err
		}
		if err := os.Remove(s.dataFD.Name()); err != nil {
			return err
		}
	}

	if err := os.RemoveAll(tmpName(s.nextGenDir)); err != nil {
		return err
	}
	if err := os.RemoveAll(tmpName(s.curGenDir)); err != nil {
		return err
	}
	return nil
}

func parentDir(dir string) string {
	return filepath.Dir(dir)
}

func tmpName(path string) string {
	return path + tmpSuffix
}

func nonTmpName(path string) string {
	return strings.TrimSuffix(path, tmpSuffix)
}

func moveFromTmpSync(src string) (string, error) {
	dst := nonTmpName(src)
	if err := os.Rename(src, dst); err != nil {
		return "", err
	}

	// Sync parent directory to ensure snapshot is visible, but it's only
	// needed on *nix style file systems.
	if runtime.GOOS != "windows" {
		if err := syncDir(parentDir(dst)); err != nil {
			return "", err
		}
	}
	return dst, nil
}

func syncDir(dir string) error {
	fh, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fh.Close()

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}
