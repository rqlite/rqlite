package chunking

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// Dechunker is a writer that writes chunks to a file and returns the file path when
// the last chunk is received and the Dechunker is closed.
type Dechunker struct {
	filePath string
	file     *os.File
	streamID string
	seqNum   int64
}

// NewDechunker returns a new Dechunker that writes chunks to a file in dir.
func NewDechunker(dir string) (*Dechunker, error) {
	file, err := os.CreateTemp(dir, "dechunker-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file in dir %s: %v", dir, err)
	}
	return &Dechunker{
		filePath: file.Name(),
		file:     file,
	}, nil
}

// WriteChunk writes the chunk to the file. If the chunk is the last chunk, the
// the bool return value is true.
func (d *Dechunker) WriteChunk(chunk *proto.LoadChunkRequest) (bool, error) {
	if d.streamID == "" {
		d.streamID = chunk.StreamId
	} else if d.streamID != chunk.StreamId {
		return false, fmt.Errorf("chunk has unexpected stream ID: expected %s but got %s", d.streamID, chunk.StreamId)
	}

	if chunk.SequenceNum != d.seqNum+1 {
		return false, fmt.Errorf("chunks received out of order: expected %d but got %d", d.seqNum+1, chunk.SequenceNum)
	}
	d.seqNum = chunk.SequenceNum

	if chunk.Data != nil {
		buf := bytes.NewBuffer(chunk.Data)
		gzw, err := gzip.NewReader(buf)
		if err != nil {
			return false, fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gzw.Close()

		if _, err := io.Copy(d.file, gzw); err != nil {
			return false, fmt.Errorf("failed to write decompressed data to file: %v", err)
		}
	}

	return chunk.IsLast, nil
}

// Close closes the Dechunker and returns the file path containing the reassembled data.
func (d *Dechunker) Close() (string, error) {
	if err := d.file.Close(); err != nil {
		return "", fmt.Errorf("failed to close file: %v", err)
	}
	return d.filePath, nil
}

// DechunkerManager manages Dechunkers.
type DechunkerManager struct {
	dir string
	mu  sync.Mutex
	m   map[string]*Dechunker
}

// NewDechunkerManager returns a new DechunkerManager.
func NewDechunkerManager(dir string) (*DechunkerManager, error) {
	// Test that we can use the given directory.
	file, err := os.CreateTemp(dir, "dechunker-manager-test-*")
	if err != nil {
		return nil, fmt.Errorf("failed to test file in dir %s: %v", dir, err)
	}
	file.Close()
	os.Remove(file.Name())

	return &DechunkerManager{
		dir: dir,
	}, nil
}

// Get returns the Dechunker for the given stream ID. If the Dechunker does not
// exist, it is created.
func (d *DechunkerManager) Get(id string) (*Dechunker, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.m == nil {
		d.m = make(map[string]*Dechunker)
	}

	if _, ok := d.m[id]; !ok {
		dechunker, err := NewDechunker(d.dir)
		if err != nil {
			return nil, err
		}
		d.m[id] = dechunker
	}

	return d.m[id], nil
}

// Delete deletes the Dechunker for the given stream ID.
func (d *DechunkerManager) Delete(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dc, ok := d.m[id]
	if ok {
		dc.Close()
		delete(d.m, id)
	}
}

// Closes closes the DechunkerManager and all Dechunkers it manages.
func (d *DechunkerManager) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, dc := range d.m {
		dc.Close()
		delete(d.m, dc.streamID)
	}
}
