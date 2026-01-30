package snapshot2

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
	"github.com/rqlite/rqlite/v9/snapshot2/proto"
	pb "google.golang.org/protobuf/proto"
)

var (
	// ErrTooManyWALs indicates that the snapshot contains more WAL files than expected.
	ErrTooManyWALs = fmt.Errorf("too many WAL files")
)

type sinker interface {
	Open() error
	io.WriteCloser
}

type fullChecker interface {
	FullNeeded() (bool, error)
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

	fn fullChecker

	logger *log.Logger
}

// NewSink creates a new Sink object. It takes the root snapshot directory
// and the snapshot metadata.
func NewSink(dir string, meta *raft.SnapshotMeta, fn fullChecker) *Sink {
	return &Sink{
		dir:    dir,
		meta:   meta,
		fn:     fn,
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

		// We have a header to figure out what to do with it.
		if s.header.DbHeader != nil {
			s.sinkW = NewFullSink(s.snapTmpDirPath, s.header)
		} else if s.header.WalHeaders != nil {
			// If we only have WAL headers then we must have only one WAL header.
			// This is because it must be an incremental snapshot.
			if len(s.header.WalHeaders) != 1 {
				return n, ErrTooManyWALs
			}
			if s.fn != nil {
				fullNeeded, err := s.fn.FullNeeded()
				if err != nil {
					return n, err
				}
				if fullNeeded {
					return n, fmt.Errorf("full snapshot needed before incremental can be applied")
				}
			}
			s.sinkW = NewIncrementalSink(s.snapTmpDirPath, s.header.WalHeaders[0])
		} else {
			return n, fmt.Errorf("unrecognized snapshot header")
		}

		// Prep and preload the sink.
		if err := s.sinkW.Open(); err != nil {
			return n, err
		}
		s.buf.WriteTo(s.sinkW)
		return n, nil
	}

	// We have a header, just write directly to the underlying sink.
	return s.sinkW.Write(p)
}

// Close closes the sink.
func (s *Sink) Close() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	if err := s.sinkW.Close(); err != nil {
		return err
	}

	if err := writeMeta(s.snapTmpDirPath, s.meta); err != nil {
		return err
	}

	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return err
	}
	return syncDirMaybe(s.snapDirPath)
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
	hdrPrefixSz := proto.HeaderSizeLen
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

	if err := fh.Sync(); err != nil {
		return err
	}
	return fh.Close()
}
