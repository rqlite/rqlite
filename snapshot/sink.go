package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/rsum"
	"github.com/rqlite/rqlite/v10/snapshot/proto"
	pb "google.golang.org/protobuf/proto"
)

type sinker interface {
	Open() error
	io.WriteCloser
}

type fullController interface {
	FullNeeded() (bool, error)
	UnsetFullNeeded() error
}

// Sink is a sink for writing snapshot data to a Snapshot store.
//
// This sink is adaptive. It can handle multiple types of snapshot data,
// including full database snapshots, incremental snapshots (WAL files),
// and snapshot installs (DB file plus WAL files).
type Sink struct {
	dir    string
	meta   *raft.SnapshotMeta
	opened bool

	snapDirPath    string
	snapTmpDirPath string

	buf    bytes.Buffer
	header *proto.SnapshotHeader
	sinkW  sinker

	// localWALDir is set when the header indicates an IncrementalFileSnapshot.
	// In this case, no data follows the header; the WAL directory is moved on Close.
	localWALDir string

	fc fullController

	logger *log.Logger
}

// NewSink creates a new Sink object. It takes the root snapshot directory
// and the snapshot metadata.
func NewSink(dir string, meta *raft.SnapshotMeta, fc fullController) *Sink {
	return &Sink{
		dir:    dir,
		meta:   meta,
		fc:     fc,
		logger: log.New(log.Writer(), "[snapshot-sink] ", log.LstdFlags),
	}
}

// Open opens the sink for writing.
func (s *Sink) Open() error {
	if s.opened {
		return nil
	}
	s.opened = true

	// Make temp snapshot directory
	s.snapDirPath = filepath.Join(s.dir, s.meta.ID)
	s.snapTmpDirPath = tmpName(s.snapDirPath)
	if err := os.MkdirAll(s.snapTmpDirPath, 0755); err != nil {
		return err
	}
	return nil
}

// ID returns the ID of the snapshot.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Write writes snapshot data to the sink.
func (s *Sink) Write(p []byte) (n int, err error) {
	// If we don't yet have a header, try to decode one.
	if s.header == nil {
		n, err := s.buf.Write(p)
		if err != nil {
			return n, err
		}

		if err := s.processHeader(); err != nil {
			return n, err
		}

		if s.header == nil {
			// Still waiting for more data to form a complete header.
			return n, nil
		}

		// We have a header, figure out what to do with it.
		switch p := s.header.Payload.(type) {
		case *proto.SnapshotHeader_Full:
			s.sinkW = NewFullSink(s.snapTmpDirPath, p.Full)
		case *proto.SnapshotHeader_Incremental:
			if s.fc != nil {
				fullNeeded, err := s.fc.FullNeeded()
				if err != nil {
					return n, err
				}
				if fullNeeded {
					return n, fmt.Errorf("full snapshot needed before incremental can be applied")
				}
			}
			s.sinkW = NewIncrementalSink(s.snapTmpDirPath, p.Incremental.WalHeader)
		case *proto.SnapshotHeader_IncrementalFile:
			if s.fc != nil {
				fullNeeded, err := s.fc.FullNeeded()
				if err != nil {
					return n, err
				}
				if fullNeeded {
					return n, fmt.Errorf("full snapshot needed before incremental can be applied")
				}
			}
			// No data follows this header type. Any leftover bytes are an error.
			if s.buf.Len() > 0 {
				return n, fmt.Errorf("unexpected data after incremental file header")
			}
			s.localWALDir = p.IncrementalFile.WalDirPath
			return n, nil
		default:
			return n, fmt.Errorf("unrecognized snapshot header payload")
		}

		// Prep and preload the streaming sink.
		if err := s.sinkW.Open(); err != nil {
			return n, err
		}
		if _, err := s.buf.WriteTo(s.sinkW); err != nil {
			return n, err
		}
		return n, nil
	}

	// We have a header, just write directly to the underlying sink.
	if s.sinkW == nil {
		// IncrementalFile path — no data expected after header.
		return 0, fmt.Errorf("unexpected data after incremental file header")
	}
	return s.sinkW.Write(p)
}

// Close closes the sink.
func (s *Sink) Close() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	if s.sinkW == nil && s.localWALDir == "" {
		// Header was never fully received; clean up the temp directory.
		return os.RemoveAll(s.snapTmpDirPath)
	}

	if s.localWALDir != "" {
		// IncrementalFileSnapshot: atomically move the WAL directory into the
		// snapshot directory, then redistribute the WAL files.
		movedDir := filepath.Join(s.snapTmpDirPath, "wal-incoming")
		if err := os.Rename(s.localWALDir, movedDir); err != nil {
			return err
		}

		walMatches, err := filepath.Glob(filepath.Join(movedDir, "*"+walfileSuffix))
		if err != nil {
			return fmt.Errorf("globbing for WAL files in moved directory: %w", err)
		}
		if len(walMatches) > 1 {
			s.logger.Printf("found %d WAL files in moved directory, processing multi-WAL snapshot",
				len(walMatches))
		}

		for _, srcPath := range walMatches {
			if !db.IsValidSQLiteWALFile(srcPath) {
				return fmt.Errorf("%s is not a valid SQLite WAL file", srcPath)
			}

			srcCRCPath := srcPath + crcSuffix
			ok, err := rsum.CompareCRC32SumFile(srcPath, srcCRCPath)
			if err != nil {
				return fmt.Errorf("comparing CRC32 sum for %s: %w", srcPath, err)
			}
			if !ok {
				return fmt.Errorf("CRC32 sum mismatch for %s", srcPath)
			}

			name := filepath.Base(srcPath)
			dstPath := filepath.Join(s.snapTmpDirPath, name)
			if err := os.Rename(srcPath, dstPath); err != nil {
				return err
			}
			if err := os.Rename(srcCRCPath, dstPath+crcSuffix); err != nil {
				return err
			}
		}

		if err := os.Remove(movedDir); err != nil {
			return err
		}
	} else {
		if err := s.sinkW.Close(); err != nil {
			return err
		}
	}

	if err := writeMeta(s.snapTmpDirPath, s.meta); err != nil {
		return err
	}

	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return err
	}

	if s.fc != nil {
		if err := s.fc.UnsetFullNeeded(); err != nil {
			return err
		}
	}
	return syncDirMaybe(s.dir)
}

// Cancel cancels the sink.
func (s *Sink) Cancel() error {
	if !s.opened {
		return nil
	}
	s.opened = false
	if s.sinkW != nil {
		if err := s.sinkW.Close(); err != nil {
			return err
		}
		s.sinkW = nil
	}
	return os.RemoveAll(s.snapTmpDirPath)
}

// processHeader processes the header data in the buffer to extract the header.
// When the header is successfully extracted, the Sink's header pointer is set.
// There may still be remaining data in the buffer after the header is formed and
// it is up to the caller to handle that data appropriately. However the buffer
// will contain only unprocessed data after this function returns.
func (s *Sink) processHeader() error {
	hdrPrefixSz := HeaderSizeLen
	if s.buf.Len() < hdrPrefixSz {
		// Not enough data to read length prefix.
		return nil
	}

	// Read length prefix encoded big endian
	numHdrBytes := binary.BigEndian.Uint32(s.buf.Bytes()[:hdrPrefixSz])
	if s.buf.Len() < hdrPrefixSz+int(numHdrBytes) {
		// Not enough data to read complete header.
		return nil
	}

	// We have enough data to read the header.
	headerBytes := s.buf.Bytes()[hdrPrefixSz : hdrPrefixSz+int(numHdrBytes)]
	header := &proto.SnapshotHeader{}
	if err := pb.Unmarshal(headerBytes, header); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot header: %v", err)
	}
	s.header = header

	// Remove processed data from buffer.
	remainingBytes := s.buf.Bytes()[hdrPrefixSz+int(numHdrBytes):]
	s.buf.Reset()
	s.buf.Write(remainingBytes)
	return nil
}

// writeMeta is used to write the meta data in a given snapshot directory.
func writeMeta(dir string, meta *raft.SnapshotMeta) error {
	fh, err := os.Create(metaPath(dir))
	if err != nil {
		return fmt.Errorf("error creating meta file: %v", err)
	}
	defer fh.Close()

	// Write out as JSON
	enc := json.NewEncoder(fh)
	if err = enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode meta: %v", err)
	}

	return fh.Sync()
}
