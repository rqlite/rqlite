package snapshot9

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/snapshot9/proto"
	pb "google.golang.org/protobuf/proto"
)

// LockingSink is a wrapper around a SnapshotSink holds the CAS lock
// while the Sink is in use.
type LockingSink struct {
	raft.SnapshotSink
	str *ReferentialStore

	mu     sync.Mutex
	closed bool
}

// NewLockingSink returns a new LockingSink.
func NewLockingSink(sink raft.SnapshotSink, str *ReferentialStore) *LockingSink {
	return &LockingSink{
		SnapshotSink: sink,
		str:          str,
	}
}

// Close closes the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndWrite()
	return s.SnapshotSink.Close()
}

// Cancel cancels the sink, unlocking the Store for creation of a new sink.
func (s *LockingSink) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.str.mrsw.EndWrite()
	return s.SnapshotSink.Cancel()
}

// Sink is a sink for writing snapshot data to a Snapshot store.
type Sink struct {
	str  *ReferentialStore
	meta *raft.SnapshotMeta

	snapDirPath    string
	snapTmpDirPath string
	dataFD         *os.File
	opened         bool

	proofLengthRead      bool
	proofLength          uint64
	proofBuffer          []byte
	proofBytesReadLength uint64
}

// NewSink creates a new Sink object.
func NewSink(str *ReferentialStore, meta *raft.SnapshotMeta) *Sink {
	return &Sink{
		str:  str,
		meta: meta,
	}
}

// Open opens the sink for writing.
func (s *Sink) Open() error {
	if s.opened {
		return nil
	}
	s.opened = true

	// Make temp snapshot directory
	s.snapDirPath = filepath.Join(s.str.Dir(), s.meta.ID)
	s.snapTmpDirPath = tmpName(s.snapDirPath)
	if err := os.MkdirAll(s.snapTmpDirPath, 0755); err != nil {
		return err
	}

	dataPath := filepath.Join(s.snapTmpDirPath, dataFileName)
	dataFD, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	s.dataFD = dataFD

	return nil
}

// Write writes snapshot data to the sink. The snapshot is not in place
// until Close is called.
func (s *Sink) Write(p []byte) (n int, err error) {
	totalWritten := 0

	if !s.proofLengthRead {
		// Read the first proto.ProtobufLength bytes to get the length of the Proof protobuf
		remainingLengthBytes := proto.ProtobufLength - len(s.proofBuffer)
		toCopy := min(remainingLengthBytes, len(p))
		s.proofBuffer = append(s.proofBuffer, p[:toCopy]...)
		p = p[toCopy:]
		totalWritten += toCopy

		if len(s.proofBuffer) < proto.ProtobufLength {
			// We need more data, return so that Write() can be called again.
			return totalWritten, nil
		}

		// We have the proto length!
		s.proofLength = binary.LittleEndian.Uint64(s.proofBuffer)
		s.proofLengthRead = true
		s.proofBuffer = make([]byte, 0, s.proofLength)
	}

	if s.proofLengthRead && s.proofBytesReadLength < s.proofLength {
		// Read the Proof protobuf
		remainingProofBytes := s.proofLength - s.proofBytesReadLength
		toCopy := min(remainingProofBytes, uint64(len(p)))
		s.proofBuffer = append(s.proofBuffer, p[:toCopy]...)
		p = p[toCopy:]
		s.proofBytesReadLength += toCopy
		totalWritten += int(toCopy)

		if len(s.proofBuffer) < int(s.proofLength) {
			// We need more data, return so that Write() can be called again.
			return totalWritten, nil
		}

		// We have enough data to decode the proto!
		var proof proto.Proof
		if err := pb.Unmarshal(s.proofBuffer, &proof); err != nil {
			return totalWritten, err
		}
		if err := s.writeProof(s.snapTmpDirPath, &proof); err != nil {
			return totalWritten, err
		}
	}

	if s.proofBytesReadLength == s.proofLength {
		n, err := io.Copy(s.dataFD, bytes.NewReader(p))
		if err != nil {
			return totalWritten, err
		}
		totalWritten += int(n)
	}

	return totalWritten, nil
}

// ID returns the ID of the snapshot being written.
func (s *Sink) ID() string {
	return s.meta.ID
}

// Cancel cancels the snapshot. Cancel must be called if the snapshot is not
// going to be closed.
func (s *Sink) Cancel() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	if err := s.dataFD.Close(); err != nil {
		return err
	}
	return RemoveAllTmpSnapshotData(s.str.Dir())
}

// Close closes the sink, and finalizes creation of the snapshot. It is critical
// that Close is called, or the snapshot will not be in place. It is OK to call
// Close without every calling Write. In that case the Snapshot will be finalized
// as usual, but will effectively be the same as the previously created snapshot.
func (s *Sink) Close() error {
	if !s.opened {
		return nil
	}
	s.opened = false

	// Write meta data, setting the total size of the Snapshot.
	if err := s.writeMeta(s.snapTmpDirPath); err != nil {
		return err
	}
	proof, err := readProof(s.snapTmpDirPath)
	if err != nil {
		return err
	}
	// XXX Need to decide what is the size of a snapshot. Size of Proof on disk as JSON? Or marshalled as pure bytes?
	if err := updateMetaSize(s.snapTmpDirPath, int64(proto.ProtobufLength+proof.SizeBytes)); err != nil {
		return fmt.Errorf("failed to update snapshot meta size: %s", err.Error())
	}

	if err := s.dataFD.Close(); err != nil {
		return err
	}
	if err := removeIfEmpty(s.dataFD.Name()); err != nil {
		return err
	}

	// Indicate snapshot data been successfully persisted to disk by renaming
	// the temp directory to a non-temporary name.
	if err := os.Rename(s.snapTmpDirPath, s.snapDirPath); err != nil {
		return err
	}
	if err := syncDirMaybe(s.str.Dir()); err != nil {
		return err
	}

	return nil
}

func (s *Sink) writeMeta(dir string) error {
	return writeMeta(dir, s.meta)
}

func (s *Sink) writeProof(dir string, proof *proto.Proof) error {
	return writeProof(dir, proof)
}
