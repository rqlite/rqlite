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
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/snapshot/proto"
	pb "google.golang.org/protobuf/proto"
)

type sinker interface {
	Open() error
	io.WriteCloser
}

type snapshotTypeController interface {
	DueNext() (Type, error)
	SetDueNext(Type) error
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

	stc snapshotTypeController

	// closeCh, when non-nil, receives a non-blocking signal after a
	// successful Close.
	closeCh chan<- struct{}

	// fatalFn is called when Close encounters an error during an incremental
	// snapshot. In production this terminates the process to avoid data
	// corruption; in tests it can be set to nil so errors propagate normally.
	fatalFn func(error)

	logger *log.Logger
}

// NewSink creates a new Sink object. It takes the root snapshot directory
// and the snapshot metadata. closeCh, if non-nil, will receive a non-blocking
// signal after a successful Close.
func NewSink(dir string, meta *raft.SnapshotMeta, stc snapshotTypeController, closeCh chan<- struct{}) *Sink {
	logger := log.New(log.Writer(), "[snapshot-sink] ", log.LstdFlags)
	return &Sink{
		dir:     dir,
		meta:    meta,
		stc:     stc,
		closeCh: closeCh,
		fatalFn: func(err error) {
			logger.Fatalf("failure during incremental snapshot, exiting process to avoid data corruption: %v", err)
		},
		logger: logger,
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
//
// If the sink is handling a Full snapshot, this function writes the data to
// the Snapshot store as it is received. If the sink is handling an IncrementalFile
// snapshot, this function expects no data to be written after the header, and
// simply records the path to the WAL directory for processing on Close.
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
		case *proto.SnapshotHeader_IncrementalFile:
			if s.stc != nil {
				dueNext, err := s.stc.DueNext()
				if err != nil {
					return n, err
				}
				if dueNext == Full {
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
//
// If the sink is handling a Full snapshot, this function closes the underlying
// streaming sink, writes the snapshot metadata, and moves the temp snapshot
// directory into place. Once it returns successfully a new snapshot has been
// installed.
//
// If the sink is handling an IncrementalFile snapshot, this function moves the
// WAL directory into the snapshot directory, writes the snapshot metadata, and
// moves the temp snapshot directory into place. In this context Close breaks the
// series of WAL files, so if there is an error during this process it means that
// future WAL files cannot be applied to the snapshot. This is a critical failure
// and we have two ways of responding to it: require a FullSnapshot next time round
// or just hard exit. On startup the node will recover from the last successfully
// installed snapshot, and the end result will be the same. However the experience
// is better if we hard exit. Why? Because requiring a full snapshot will mean a
// longer pause of write traffic, while a hard restart will mean the node will go
// down, indicating to the rest of the cluster it has failed. Assuming the cluster
// still has quorum, this means writes won't be blocked during the restart.
//
// Ultimately it's a judgment call. This entire error scenario shouldn't happen
// unless there is some kind of disk or permissions failure, in which case the node
// is likely in bad shape anyway.
func (s *Sink) Close() (retErr error) {
	if !s.opened {
		return nil
	}
	s.opened = false

	if s.sinkW == nil && s.localWALDir == "" {
		// Header was never fully received; clean up the temp directory.
		return os.RemoveAll(s.snapTmpDirPath)
	}

	defer func() {
		if retErr != nil {
			stats.Add(sinkErrors, 1)
			if s.localWALDir != "" && s.fatalFn != nil {
				s.fatalFn(retErr)
			}
		} else if s.localWALDir != "" {
			stats.Add(sinkIncrementalTotal, 1)
		} else if s.sinkW != nil {
			stats.Add(sinkFullTotal, 1)
		}
	}()

	if s.localWALDir != "" {
		// IncrementalFileSnapshot: atomically move the WAL directory into the
		// snapshot directory, then redistribute the WAL files.
		movedDir := filepath.Join(s.snapTmpDirPath, "wal-incoming")
		if err := os.Rename(s.localWALDir, movedDir); err != nil {
			return fmt.Errorf("failed to move WAL directory into snapshot directory: %v", err)
		}
		sd := NewStagingDir(movedDir)
		if err := sd.MoveWALFilesTo(s.snapTmpDirPath); err != nil {
			return fmt.Errorf("failed to move WAL files into snapshot directory: %v", err)
		}
		if err := os.Remove(movedDir); err != nil {
			return fmt.Errorf("failed to remove temporary WAL directory: %v", err)
		}
	} else {
		if err := s.sinkW.Close(); err != nil {
			return fmt.Errorf("failed to close sink: %v", err)
		}
	}

	if err := writeMeta(s.snapTmpDirPath, s.meta); err != nil {
		return fmt.Errorf("failed to write meta: %v", err)
	}

	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return fmt.Errorf("failed to rename snapshot directory: %v", err)
	}

	if s.stc != nil {
		if err := s.stc.SetDueNext(Incremental); err != nil {
			return fmt.Errorf("failed to set due next to incremental: %v", err)
		}
	}
	if err := fsutil.SyncDirMaybe(s.dir); err != nil {
		return err
	}

	if s.closeCh != nil {
		select {
		case s.closeCh <- struct{}{}:
		default:
		}
	}
	return nil
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
