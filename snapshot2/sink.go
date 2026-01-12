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

const (
	manifestHdrLen = 4
)

type sinker interface {
	Open() error
	io.WriteCloser
}

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	dir    string
	meta   *raft.SnapshotMeta
	opened bool

	snapDirPath    string
	snapTmpDirPath string

	buf      bytes.Buffer
	manifest *proto.SnapshotManifest
	sinkW    sinker

	logger *log.Logger
}

// NewSink creates a new Sink object. It takes the root snapshot directory
// and the snapshot metadata.
func NewSink(dir string, meta *raft.SnapshotMeta) *Sink {
	return &Sink{
		dir:    dir,
		meta:   meta,
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

	// If we don't yet have a manifest, try to decode one.
	if s.manifest == nil {
		n, err := s.buf.Write(p)
		if err != nil {
			return n, err
		}

		if err := s.processHeader(); err != nil {
			return n, err
		}

		if s.manifest == nil {
			// Still waiting for more data to form a complete header.
			return n, nil
		}

		// We have a manifest to figure out what to do with it.
		if s.manifest.GetDbPath() != nil {
			s.sinkW = NewDBSink(s.snapTmpDirPath, s.manifest.GetDbPath())
		} else if s.manifest.GetWalPath() != nil {
			s.sinkW = NewWALSink(s.snapTmpDirPath, s.manifest.GetWalPath())
		} else if s.manifest.GetInstall() != nil {
			s.sinkW = NewInstallSink(s.snapTmpDirPath, s.manifest.GetInstall())
		} else {
			return n, fmt.Errorf("unrecognized snapshot manifest")
		}

		// Prep and preload the sink.
		if err := s.sinkW.Open(); err != nil {
			return n, err
		}
		s.buf.WriteTo(s.sinkW)
		return n, nil
	}

	// We have a manifest, just write directly to the underlying sink.
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

	// if err := writeMeta(s.snapTmpDirPath, s.meta); err != nil {
	// 	return err
	// }

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

// processHeader processes the header data in the buffer to extract the manifest.
// When the manifest is successfully extracted, the Sink's manifest pointer is set.
// There may still be remaining data in the buffer after the manifest is formed and
// it is up to the caller to handle that data appropriately. Howveer the buffer
// will contain only unprocessed data after this function returns.
func (s *Sink) processHeader() error {
	if s.buf.Len() < manifestHdrLen {
		// Not enough data to read length prefix.
		return nil
	}

	// Read length prefix encoded big endian
	lenBytes := binary.BigEndian.Uint32(s.buf.Bytes()[:manifestHdrLen])
	if uint32(s.buf.Len()) < manifestHdrLen+lenBytes {
		// Not enough data to read complete manifest.
		return nil
	}

	// We have enough data to read the manifest.
	manifestBytes := s.buf.Bytes()[manifestHdrLen : manifestHdrLen+lenBytes]
	manifest := &proto.SnapshotManifest{}
	if err := pb.Unmarshal(manifestBytes, manifest); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot manifest: %v", err)
	}
	s.manifest = manifest

	// Remove processed data from buffer.
	remainingBytes := s.buf.Bytes()[manifestHdrLen+lenBytes:]
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
