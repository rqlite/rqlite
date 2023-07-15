package chunking

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/golang/snappy"
	"github.com/rqlite/rqlite/command"
)

const (
	internalChunkSize = 1024 * 1024
)

// Compression is the type of compression to use. XXX move to protos!
type Compression int

const (
	None Compression = iota
	Gzip
	Snappy
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
		gw, err := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		if err != nil {
			panic(fmt.Sprintf("failed to create gzip writer: %s", err.Error()))
		}
		return gw
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
				c.finished = true
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
			Compression: command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP,
			Data:        nil,
		}, nil
	}

	c.sequenceNum++
	return &command.LoadChunkRequest{
		StreamId:    c.streamID,
		SequenceNum: c.sequenceNum,
		IsLast:      totalRead < c.chunkSize,
		Compression: command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP,
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

// ParallelChunker is a reader that reads from an underlying io.Reader and returns
// LoadChunkRequests of a given size.
type ParallelChunker struct {
	r           *CountingReader
	chunkSize   int64
	parallelism int
	compAlgo    command.LoadChunkRequest_Compression

	streamID    string
	sequenceNum int64
}

// NewParallelChunker returns a new ParallelChunker that reads from r and returns
// LoadChunkRequests of size chunkSize.
func NewParallelChunker(r io.Reader, chunkSz int64, parallelism int, comp command.LoadChunkRequest_Compression) *ParallelChunker {
	return &ParallelChunker{
		r:           NewCountingReader(r),
		chunkSize:   chunkSz,
		parallelism: parallelism,
		compAlgo:    comp,
		streamID:    generateStreamID(),
		sequenceNum: 1,
	}
}

func (c *ParallelChunker) Start() <-chan *command.LoadChunkRequest {
	out := make(chan *command.LoadChunkRequest)
	go c.readChunks(out)
	return out
}

func (c *ParallelChunker) readChunks(out chan<- *command.LoadChunkRequest) {
	toCompressCh := make(chan *command.LoadChunkRequest, c.parallelism)
	compressedCh := make(chan *command.LoadChunkRequest, c.parallelism)

	// Start the parallel compressor goroutines.
	wg := sync.WaitGroup{}
	for i := 0; i < c.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range toCompressCh {
				switch c.compAlgo {
				case command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_NONE:
					// Nothing to do
				case command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP:
					buf := new(bytes.Buffer)
					gw := gzip.NewWriter(buf)
					if _, err := gw.Write(chunk.Data); err != nil {
						panic(err)
					}
					if err := gw.Close(); err != nil {
						panic(err)
					}
					chunk.Data = buf.Bytes()
					chunk.Compression = command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP
				case command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_SNAPPY:
					buf := new(bytes.Buffer)
					sw := snappy.NewBufferedWriter(buf)
					if _, err := sw.Write(chunk.Data); err != nil {
						panic(err)
					}
					if err := sw.Close(); err != nil {
						panic(err)
					}
					chunk.Data = buf.Bytes()
					chunk.Compression = command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_SNAPPY
				default:
					panic("unknown compression algorithm")
				}

				compressedCh <- chunk
			}
		}()
	}

	// Run the goroutine that reads from the compressor goroutines and
	// ensures the chunks are sent in order.
	go sortChunks(c.sequenceNum, compressedCh, out)

	for {
		buf := new(bytes.Buffer)
		n, err := io.CopyN(buf, c.r, c.chunkSize)
		if err != nil && err != io.EOF {
			panic(err)
		}

		chunk := &command.LoadChunkRequest{
			StreamId:    c.streamID,
			SequenceNum: c.sequenceNum,
			IsLast:      n < c.chunkSize,
			Data:        buf.Bytes(),
		}
		toCompressCh <- chunk
		c.sequenceNum++

		if chunk.IsLast {
			break
		}
	}
	close(toCompressCh)
	wg.Wait()
	close(compressedCh)
}

func sortChunks(firstSeqNum int64, compressedCh <-chan *command.LoadChunkRequest, out chan<- *command.LoadChunkRequest) {
	var chunks []*command.LoadChunkRequest
	nextSeq := firstSeqNum

	for c := range compressedCh {
		chunks = append(chunks, c)
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].SequenceNum < chunks[j].SequenceNum })

		for len(chunks) > 0 && chunks[0].SequenceNum == nextSeq {
			chunk := chunks[0]
			out <- chunk
			if chunk.IsLast {
				close(out)
				return
			}

			chunks = chunks[1:]
			nextSeq++
		}
	}
}

func (c *ParallelChunker) Counts() (int64, int64, int64) { /// XXXX needs work
	return c.sequenceNum, c.r.Count(), 0
}
