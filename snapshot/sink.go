package snapshot

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rqlite/rqlite/db"
	"google.golang.org/protobuf/proto"
)

type Sink struct {
	str         *Store
	dir         string
	walFilePath string

	dataFD *os.File
	meta   *Meta

	logger *log.Logger
	closed bool
}

func NewSink(s *Store, dir string, meta *Meta) *Sink {
	tDir := tmpName(dir)
	return &Sink{
		str:         s,
		dir:         tDir,
		walFilePath: filepath.Join(tDir, snapWALFile),
		meta:        meta,
		logger:      log.New(os.Stderr, "snapshot-sink: ", log.LstdFlags),
	}
}

func (s *Sink) Open() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return err
	}
	return nil
}

func (s *Sink) Write(p []byte) (n int, err error) {
	if s.dataFD == nil {
		f, err := os.CreateTemp(s.dir, "snapshot-data.tmp")
		if err != nil {
			return 0, err
		}
		s.dataFD = f
	}
	return s.dataFD.Write(p)
}

func (s *Sink) ID() string {
	return s.meta.ID
}

func (s *Sink) Cancel() error {
	s.closed = true
	if s.dataFD != nil {
		s.dataFD.Close()
	}
	return nil
}

func (s *Sink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	if s.dataFD != nil {
		defer s.dataFD.Close()
		if err := s.processSnapshotData(); err != nil {
			return err
		}
	}

	return os.Rename(s.dir, nonTmpName(s.dir))
}

func (s *Sink) processSnapshotData() error {
	if _, err := s.dataFD.Seek(0, 0); err != nil {
		return err
	}

	strHdr, err := getStreamHeader(s.dataFD)
	if err != nil {
		return fmt.Errorf("error unmarshaling FSM snapshot: %v", err)
	}

	// Incremental snapshot?
	if incSnap := strHdr.GetIncrementalSnapshot(); incSnap != nil {
		if err := s.processIncrementalSnapshot(incSnap); err != nil {
			return err
		}
	} else {
		// Turns out it's a full snapshot.
		fullSnap := strHdr.GetFullSnapshot()
		if fullSnap == nil {
			return fmt.Errorf("got nil FullSnapshot")
		}
		if err := s.processFullSnapshot(fullSnap); err != nil {
			return err
		}
	}

	dstDir, err := moveFromTmp(s.dir)
	if err != nil {
		s.logger.Printf("failed to move snapshot directory into place: %s", err)
		return err
	}

	// Sync parent directory to ensure snapshot is visible, but it's only
	// needed on *nix style file systems.
	if runtime.GOOS != "windows" {
		if err := syncDir(parentDir(s.dir)); err != nil {
			s.logger.Printf("failed syncing parent directory: %s", err)
			return err
		}
	}

	s.logger.Printf("snapshot (ID %s) written to %s", s.meta.ID, dstDir)
	return nil
}

func (s *Sink) processIncrementalSnapshot(incSnap *IncrementalSnapshot) error {
	s.logger.Printf("processing incremental snapshot")
	if err := os.WriteFile(s.walFilePath, incSnap.Data, 0644); err != nil {
		return fmt.Errorf("error writing WAL data: %v", err)
	}
	if err := s.writeMeta(false); err != nil {
		return err
	}

	return nil
}

func (s *Sink) processFullSnapshot(fullSnap *FullSnapshot) error {
	s.logger.Printf("processing full snapshot")
	ngDir, err := s.str.GetNextGenerationDir()
	if err != nil {
		return fmt.Errorf("error getting next generation directory: %v", err)
	}
	newDir := filepath.Join(ngDir, filepath.Base(s.dir))
	if err := os.MkdirAll(newDir, 0755); err != nil {
		return fmt.Errorf("error creating full snapshot directory: %v", err)
	}
	if err := os.Rename(s.dir, newDir); err != nil {
		return fmt.Errorf("error moving full snapshot directory to %s: %v", newDir, err)
	}
	s.dir = newDir

	// Write out base SQLite file.
	dbInfo := fullSnap.GetDb()
	if dbInfo == nil {
		return fmt.Errorf("got nil DB info")
	}
	sqliteBaseFD, err := os.Create(filepath.Join(s.dir, baseSqliteFile))
	if err != nil {
		return fmt.Errorf("error creating SQLite file: %v", err)
	}
	if _, err := io.CopyN(sqliteBaseFD, s.dataFD, dbInfo.Size); err != nil {
		return fmt.Errorf("error writing SQLite file data: %v", err)
	}
	sqliteBaseFD.Close()

	// Write out WALs.
	var walFiles []string
	for i, wal := range fullSnap.GetWals() {
		if wal == nil {
			return fmt.Errorf("got nil WAL")
		}

		walName := filepath.Join(s.dir, baseSqliteWALFile+fmt.Sprintf("%d", i))
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

	// Checkpoint the WAL file into the base SQLite file
	if err := db.ReplayWAL(filepath.Join(s.dir, baseSqliteFile), walFiles, false); err != nil {
		return fmt.Errorf("error checkpointing WAL: %v", err)
	}

	if err := s.writeMeta(false); err != nil {
		return err
	}
	return nil
}

func (s *Sink) writeMeta(full bool) error {
	fh, err := os.Create(filepath.Join(s.dir, metaFileName))
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

func getStreamHeader(r io.Reader) (*StreamHeader, error) {
	b := make([]byte, strHeaderLenSize)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return nil, fmt.Errorf("error reading snapshot header length: %v", err)
	}
	strHdrLen := binary.LittleEndian.Uint64(b)

	b = make([]byte, strHdrLen)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, fmt.Errorf("error reading snapshot header %v", err)
	}
	strHdr := &StreamHeader{}
	err = proto.Unmarshal(b, strHdr)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling FSM snapshot: %v", err)
	}
	return strHdr, nil
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

func moveFromTmp(src string) (string, error) {
	dst := nonTmpName(src)
	if err := os.Rename(src, dst); err != nil {
		return "", err
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
