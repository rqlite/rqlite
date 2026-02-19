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

	// localWALPath is set when the header indicates an IncrementalFileSnapshot.
	// In this case, no data follows the header; the WAL file is moved on Close.
	localWALPath string

	// isNoop is set when the header indicates a NoopSnapshot.
	// No data follows the header; a data.noop sentinel file is created on Close.
	isNoop bool

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
			s.localWALPath = p.IncrementalFile.WalPaths[0]
			return n, nil
		case *proto.SnapshotHeader_Noop:
			if s.fc != nil {
				fullNeeded, err := s.fc.FullNeeded()
				if err != nil {
					return n, err
				}
				if fullNeeded {
					return n, fmt.Errorf("full snapshot needed before noop can be applied")
				}
			}
			// No data follows this header type. Any leftover bytes are an error.
			if s.buf.Len() > 0 {
				return n, fmt.Errorf("unexpected data after noop header")
			}
			s.isNoop = true
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
		// IncrementalFile or Noop path — no data expected after header.
		if s.isNoop {
			return 0, fmt.Errorf("unexpected data after noop header")
		}
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

	if s.sinkW == nil && s.localWALPath == "" && !s.isNoop {
		// Header was never fully received; clean up the temp directory.
		return os.RemoveAll(s.snapTmpDirPath)
	}

	if s.isNoop {
		// NoopSnapshot: create the data.noop sentinel file.
		noopPath := filepath.Join(s.snapTmpDirPath, noopfileName)
		f, err := os.Create(noopPath)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	} else if s.localWALPath != "" {
		// IncrementalFileSnapshot: move the local WAL file into the snapshot directory.
		dstPath := filepath.Join(s.snapTmpDirPath, walfileName)
		if err := os.Rename(s.localWALPath, dstPath); err != nil {
			return err
		}
		if !db.IsValidSQLiteWALFile(dstPath) {
			return fmt.Errorf("file is not a valid SQLite WAL file")
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
