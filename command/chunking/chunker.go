package chunking

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/golang/snappy"
	"github.com/rqlite/rqlite/command"
)

const (
	internalChunkSize = 1024 * 1024
)

// Define a sync.Pool to pool the buffers.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

// Define a sync.Pool to pool the snappy writers.
var snappyWriterPool = sync.Pool{
	New: func() interface{} {
		return snappy.NewBufferedWriter(nil)
	},
}

// Chunker is a reader that reads from an underlying io.Reader and returns
// LoadChunkRequests of a given size.
type Chunker struct {
	r *CountingReader

	chunkSize   int64
	streamID    string
	sequenceNum int64
	finished    bool

	statsMu  sync.Mutex
	nWritten int64
}

// NewChunker returns a new Chunker that reads from r and returns
// LoadChunkRequests of size chunkSize.
func NewChunker(r io.Reader, chunkSize int64) *Chunker {
	return &Chunker{
		r:         NewCountingReader(r),
		chunkSize: chunkSize,
		streamID:  generateStreamID(),
	}
}

func generateStreamID() string {
	b := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	// Convert the random bytes into the format of a UUID.
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Next returns the next LoadChunkRequest, or io.EOF if finished.
func (c *Chunker) Next() (*command.LoadChunkRequest, error) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	if c.finished {
		return nil, io.EOF
	}

	// Get a buffer from the pool.
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	sw := snappyWriterPool.Get().(*snappy.Writer)
	defer snappyWriterPool.Put(sw)

	sw.Reset(buf)

	// Create an intermediate buffer to read into
	intermediateBuffer := make([]byte, min(internalChunkSize, c.chunkSize))

	var totalRead int64 = 0
	for totalRead < c.chunkSize {
		n, err := c.r.Read(intermediateBuffer)
		totalRead += int64(n)
		if n > 0 {
			if _, err = sw.Write(intermediateBuffer[:n]); err != nil {
				return nil, err
			}
		}
		if err != nil {
			if err == io.EOF {
				c.finished = true
				break
			} else {
				return nil, err
			}
		}
	}

	if err := sw.Close(); err != nil {
		return nil, errors.New("failed to close snappy writer: " + err.Error())
	}
	c.nWritten += int64(buf.Len())

	// If we didn't read any data, then we're finished
	if totalRead == 0 {
		// If no previous chunks were sent at all then signal that.
		if c.sequenceNum == 0 {
			return nil, io.EOF
		}
		// If previous chunks were sent, return a final empty chunk with IsLast = true
		return &command.LoadChunkRequest{
			StreamId:    c.streamID,
			SequenceNum: c.sequenceNum + 1,
			IsLast:      true,
			Data:        nil,
		}, nil
	}

	c.sequenceNum++
	return &command.LoadChunkRequest{
		StreamId:    c.streamID,
		SequenceNum: c.sequenceNum,
		IsLast:      totalRead < c.chunkSize,
		Data:        buf.Bytes(),
	}, nil
}

// Counts returns the number of chunks generated, bytes read, and bytes written.
func (c *Chunker) Counts() (int64, int64, int64) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return c.sequenceNum, c.r.Count(), c.nWritten
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
