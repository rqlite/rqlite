package chunking

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rqlite/rqlite/command"
)

const (
	internalChunkSize = 512 * 1024
)

// Define a sync.Pool to pool the buffers.
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

// Define a sync.Pool to pool the gzip writers.
var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

// Chunker is a reader that reads from an underlying io.Reader and returns
// LoadChunkRequests of a given size.
type Chunker struct {
	r           io.Reader
	chunkSize   int64
	streamID    string
	sequenceNum int64
	finished    bool
}

// NewChunker returns a new Chunker that reads from r and returns
// LoadChunkRequests of size chunkSize.
func NewChunker(r io.Reader, chunkSize int64) *Chunker {
	return &Chunker{
		r:         r,
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
	if c.finished {
		return nil, io.EOF
	}

	// Get a buffer from the pool.
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Get a gzip.Writer from the pool.
	gw := gzipWriterPool.Get().(*gzip.Writer)
	defer gzipWriterPool.Put(gw)

	// Reset the gzip.Writer to use the buffer.
	gw.Reset(buf)

	// Create an intermediate buffer to read into
	intermediateBuffer := make([]byte, min(internalChunkSize, c.chunkSize))

	// Read data into intermediate buffer and write to gzip.Writer
	var totalRead int64 = 0
	for totalRead < c.chunkSize {
		n, err := c.r.Read(intermediateBuffer)
		totalRead += int64(n)
		if n > 0 {
			if _, err = gw.Write(intermediateBuffer[:n]); err != nil {
				return nil, err
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
	}

	// Close the gzip.Writer to ensure all data is written to the buffer
	if err := gw.Close(); err != nil {
		return nil, errors.New("failed to close gzip writer: " + err.Error())
	}

	if totalRead < c.chunkSize {
		c.finished = true
	}

	// If we didn't write any data, then we're finished
	if totalRead == 0 {
		// If no previous chunks were sent at all signal that.
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

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
