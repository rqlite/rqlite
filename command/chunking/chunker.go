package chunking

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"

	"github.com/golang/snappy"
	"github.com/rqlite/rqlite/command"
)

const (
	internalChunkSize = 1024 * 1024
)

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

	expectedSize int64

	statsMu  sync.Mutex
	nWritten int64
}

// NewChunker returns a new Chunker that reads from r and returns
// LoadChunkRequests of size chunkSize.
func NewChunker(r io.Reader, chunkSize int64) *Chunker {
	return &Chunker{
		r:            NewCountingReader(r),
		chunkSize:    chunkSize,
		streamID:     generateStreamID(),
		expectedSize: -1,
	}
}

// SetExpectedSize sets the expected size of the final data set
// to be streamed. This is used to determine if all the data has
// been streamed.
func (c *Chunker) SetExpectedSize(sz int64) {
	c.expectedSize = sz
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
	buf := GetManualBuffer()
	defer PutManualBuffer(buf)

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
			StreamId:              c.streamID,
			SequenceNum:           c.sequenceNum + 1,
			IsLast:                true,
			TotalUncompressedSize: c.expectedSize,
			Compression:           command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP,
			Data:                  nil,
		}, nil
	}

	c.sequenceNum++
	return &command.LoadChunkRequest{
		StreamId:              c.streamID,
		SequenceNum:           c.sequenceNum,
		IsLast:                totalRead < c.chunkSize,
		TotalUncompressedSize: c.expectedSize,
		Compression:           command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP,
		Data:                  buf.Bytes(),
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

type ParallelChunk struct {
	req *command.LoadChunkRequest
	buf *bytes.Buffer
}

func (c *ParallelChunk) LoadChunkRequest() *command.LoadChunkRequest {
	return c.req
}

func (c *ParallelChunk) Close() {
	fmt.Println("Closing chunk, releasing", c.buf.Len(), "bytes")
	PutManualBuffer(c.buf)
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

	writtenMu sync.Mutex
	nWritten  int64

	expectedSize int64

	logger *log.Logger
}

// NewParallelChunker returns a new ParallelChunker that reads from r and returns
// LoadChunkRequests of size chunkSize.
func NewParallelChunker(r io.Reader, chunkSz int64, parallelism int, comp command.LoadChunkRequest_Compression) *ParallelChunker {
	return &ParallelChunker{
		r:            NewCountingReader(r),
		chunkSize:    chunkSz,
		parallelism:  parallelism,
		compAlgo:     comp,
		streamID:     generateStreamID(),
		expectedSize: -1,
		logger:       log.New(os.Stderr, "[chunker] ", log.LstdFlags),
	}
}

// Start starts the chunker, returning a channel on which to receive
// LoadChunkRequests.
func (c *ParallelChunker) Start() <-chan *ParallelChunk {
	out := make(chan *ParallelChunk)
	go c.readChunks(out)
	return out
}

// SetExpectedSize sets the expected size of the final data set
// to be streamed. This is used to determine if all the data has
// been streamed.
func (c *ParallelChunker) SetExpectedSize(sz int64) {
	c.expectedSize = sz
}

func (c *ParallelChunker) readChunks(out chan<- *ParallelChunk) {
	toCompressCh := make(chan *ParallelChunk, c.parallelism)
	compressedCh := make(chan *ParallelChunk, c.parallelism)

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
					buf := GetManualBuffer()
					gw := gzip.NewWriter(buf)
					if _, err := gw.Write(chunk.req.Data); err != nil {
						c.logger.Printf("error writing gzip chunk: %s", err.Error())
					}
					if err := gw.Close(); err != nil {
						c.logger.Printf("error closing gzip writer: %s", err.Error())
					}
					PutManualBuffer(chunk.buf) // Return buffer containing uncompressed data.
					chunk.req.Data = buf.Bytes()
					chunk.req.Compression = command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_GZIP
				case command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_SNAPPY:
					buf := GetManualBuffer()
					sw := snappy.NewBufferedWriter(buf)
					if _, err := sw.Write(chunk.req.Data); err != nil {
						c.logger.Printf("error writing to snappy writer: %s", err.Error())
					}
					if err := sw.Close(); err != nil {
						c.logger.Printf("error closing snappy writer: %s", err.Error())
					}
					PutManualBuffer(chunk.buf) // Return buffer containing uncompressed data.
					chunk.req.Data = buf.Bytes()
					chunk.req.Compression = command.LoadChunkRequest_LOAD_CHUNK_REQUEST_COMPRESSION_SNAPPY
				default:
					c.logger.Printf("unknown compression algorithm: %d", c.compAlgo)
				}

				c.writtenMu.Lock()
				c.nWritten += int64(len(chunk.req.Data))
				c.writtenMu.Unlock()
				compressedCh <- chunk
			}
		}()
	}

	// Run the goroutine that reads from the compressor goroutines and
	// ensures the chunks are sent in order.
	go sortChunks(c.sequenceNum+1, compressedCh, out)
	for {
		buf := GetManualBuffer()
		n, err := io.CopyN(buf, c.r, c.chunkSize)
		if err != nil && err != io.EOF {
			c.logger.Printf("error reading chunk: %s", err.Error())
			break
		}

		c.sequenceNum++
		chunk := &command.LoadChunkRequest{
			StreamId:              c.streamID,
			SequenceNum:           c.sequenceNum,
			IsLast:                n < c.chunkSize,
			TotalUncompressedSize: c.expectedSize,
			Data:                  buf.Bytes(),
		}
		toCompressCh <- &ParallelChunk{req: chunk, buf: buf}

		if chunk.IsLast {
			break
		}
	}
	close(toCompressCh)
	wg.Wait()
	close(compressedCh)
}

func sortChunks(firstSeqNum int64, compressedCh <-chan *ParallelChunk, out chan<- *ParallelChunk) {
	var chunks []*ParallelChunk
	nextSeq := firstSeqNum

	defer close(out)

	for c := range compressedCh {
		chunks = append(chunks, c)
		sort.Slice(chunks, func(i, j int) bool { return chunks[i].req.SequenceNum < chunks[j].req.SequenceNum })

		for len(chunks) > 0 && chunks[0].req.SequenceNum == nextSeq {
			chunk := chunks[0]
			out <- chunk
			if chunk.req.IsLast {
				return
			}

			chunks = chunks[1:]
			nextSeq++
		}
	}
}

func (c *ParallelChunker) Counts() (int64, int64, int64) {
	return c.sequenceNum, c.r.Count(), c.nWritten
}
