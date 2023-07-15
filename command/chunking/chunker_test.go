package chunking

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"

	"github.com/rqlite/rqlite/command"
)

// Test_ChunkerEmptyReader tests that a Chunker created with an empty reader
// returns io.EOF on the first call to Next.
func Test_ChunkerEmptyReader(t *testing.T) {
	// Create an empty reader that immediately returns io.EOF
	r := bytes.NewReader([]byte{})
	chunker := NewChunker(r, 1024)

	// Expect the first call to Next to return io.EOF
	_, err := chunker.Next()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	// Further calls to Next should also return io.EOF
	_, err = chunker.Next()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

// Test_ChunkerSingleChunk tests that a Chunker created with a reader that
// contains a single chunk returns the expected chunk, when the chunk size is
// larger than the amount of data in the reader.
func Test_ChunkerSingleChunk(t *testing.T) {
	data := []byte("Hello, world!")
	chunker := NewChunker(bytes.NewReader(data), 32)

	chunk, err := chunker.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunk.SequenceNum != 1 {
		t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, 1)
	}
	if !chunk.IsLast {
		t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, true)
	}

	// Decompress the gzip data and compare it with expected
	gzipReader, err := gzip.NewReader(bytes.NewReader(chunk.Data))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decompressed := new(bytes.Buffer)
	if _, err = io.Copy(decompressed, gzipReader); err != nil {
		t.Fatalf("failed to decompress data: %v", err)
	}

	if decompressed.String() != string(data) {
		t.Errorf("unexpected chunk data: got %s, want %s", decompressed.String(), string(data))
	}

	// After all chunks are read, Next should return nil, io.EOF
	chunk, err = chunker.Next()
	if chunk != nil || err != io.EOF {
		t.Errorf("expected (nil, io.EOF), got (%v, %v)", chunk, err)
	}
}

// Test_ChunkerSingleChunkLarge tests that a Chunker created with a reader that
// contains a single chunk returns the expected chunk, when the chunk size is
// much larger than the amount of data in the reader, and is larger than the
// internal chunk size.
func Test_ChunkerSingleChunkLarge(t *testing.T) {
	data := []byte("Hello, world!")
	chunker := NewChunker(bytes.NewReader(data), internalChunkSize*3)

	chunk, err := chunker.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunk.SequenceNum != 1 {
		t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, 1)
	}
	if !chunk.IsLast {
		t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, true)
	}

	// Decompress the gzip data and compare it with expected
	gzipReader, err := gzip.NewReader(bytes.NewReader(chunk.Data))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decompressed := new(bytes.Buffer)
	if _, err = io.Copy(decompressed, gzipReader); err != nil {
		t.Fatalf("failed to decompress data: %v", err)
	}

	if decompressed.String() != string(data) {
		t.Errorf("unexpected chunk data: got %s, want %s", decompressed.String(), string(data))
	}

	// After all chunks are read, Next should return nil, io.EOF
	chunk, err = chunker.Next()
	if chunk != nil || err != io.EOF {
		t.Errorf("expected (nil, io.EOF), got (%v, %v)", chunk, err)
	}
}

// Test_ChunkerSingleChunkExact tests that a Chunker created with a reader that
// contains a single chunk returns the expected chunk, when the chunk size is
// exactly the size of amount of data in the reader.
func Test_ChunkerSingleChunkExact(t *testing.T) {
	data := []byte("Hello")
	chunker := NewChunker(bytes.NewReader(data), 5)

	chunk, err := chunker.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The expected sequence number should be 1
	if chunk.SequenceNum != 1 {
		t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, 1)
	}

	// Won't be last since chunker doesn't know the size of the data
	if chunk.IsLast {
		t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, true)
	}

	// Decompress the gzip data and compare it with expected
	gzipReader, err := gzip.NewReader(bytes.NewReader(chunk.Data))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decompressed := new(bytes.Buffer)
	if _, err = io.Copy(decompressed, gzipReader); err != nil {
		t.Fatalf("failed to decompress data: %v", err)
	}

	if decompressed.String() != string(data) {
		t.Errorf("unexpected chunk data: got %s, want %s", decompressed.String(), string(data))
	}

	// Call Next again, get a second chunk, which should be the last chunk
	chunk, err = chunker.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunk.SequenceNum != 2 {
		t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, 2)
	}
	if !chunk.IsLast {
		t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, true)
	}
	if chunk.Data != nil {
		t.Errorf("unexpected chunk data: got %v, want %v", chunk.Data, nil)
	}

	// After all chunks are read, Next should return nil, io.EOF
	chunk, err = chunker.Next()
	if chunk != nil || err != io.EOF {
		t.Errorf("expected (nil, io.EOF), got (%v, %v)", chunk, err)
	}
}

// Test_ChunkerMultiChunks tests that a Chunker created with a reader which contains
// enough data that multiple chunks should be returned, returns the expected chunks.
func Test_ChunkerMultiChunks(t *testing.T) {
	data := []byte("Hello, world!")
	chunkSize := int64(5)

	chunker := NewChunker(bytes.NewReader(data), chunkSize)

	expectedChunks := []string{
		"Hello",
		", wor",
		"ld!",
	}

	for i, expected := range expectedChunks {
		chunk, err := chunker.Next()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// The expected sequence number should be i+1
		if chunk.SequenceNum != int64(i+1) {
			t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, i+1)
		}

		// The expected IsLast value should be true only for the last chunk
		expectedIsLast := i == len(expectedChunks)-1
		if chunk.IsLast != expectedIsLast {
			t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, expectedIsLast)
		}

		// Decompress the gzip data and compare it with expected
		gzipReader, err := gzip.NewReader(bytes.NewReader(chunk.Data))
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer gzipReader.Close()

		decompressed := new(bytes.Buffer)
		if _, err = io.Copy(decompressed, gzipReader); err != nil {
			t.Fatalf("failed to decompress data: %v", err)
		}

		if decompressed.String() != expected {
			t.Errorf("unexpected chunk data: got %s, want %s", decompressed.String(), expected)
		}
	}

	// After all chunks are read, Next should return nil, io.EOF
	chunk, err := chunker.Next()
	if chunk != nil || err != io.EOF {
		t.Errorf("expected (nil, io.EOF), got (%v, %v)", chunk, err)
	}
}

type errorReader struct{}

func (r *errorReader) Read([]byte) (int, error) {
	return 0, errors.New("test error")
}

// Test_ChunkerReaderError tests that a Chunker created with a reader that
// returns an error other than io.EOF returns that error.
func Test_ChunkerReaderError(t *testing.T) {
	chunker := NewChunker(&errorReader{}, 1024)
	_, err := chunker.Next()
	if err == nil || err.Error() != "test error" {
		t.Errorf("expected test error, got %v", err)
	}
}

func test_ParallelChunk_SingleChunk(t *testing.T, numCompressors int) {
	data := []byte("Hello, world!")
	chunkSize := int64(32)

	chunker := NewParallelChunker(bytes.NewReader(data), chunkSize, numCompressors, Gzip)
	ch := chunker.Start()

	var expectedChunks []*command.LoadChunkRequest
	for chunk := range ch {
		expectedChunks = append(expectedChunks, chunk)
	}
	if len(expectedChunks) != 1 {
		t.Fatalf("unexpected number of chunks: got %d, want %d", len(expectedChunks), 1)
	}
	chunk := expectedChunks[0]

	if chunk.SequenceNum != 1 {
		t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, 1)
	}
	if !chunk.IsLast {
		t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, true)
	}

	decompressed := mustGunzip(t, chunk.Data)
	if string(decompressed) != string(data) {
		t.Errorf("unexpected chunk data: got %s, want %s", string(decompressed), string(data))
	}
}

func Test_ParallelChunker_SingleChunk(t *testing.T) {
	t.Run("SingleCompressor", func(t *testing.T) {
		test_ParallelChunk_SingleChunk(t, 1)
	})
	t.Run("MultiCompressor", func(t *testing.T) {
		test_ParallelChunk_SingleChunk(t, 8)
	})
}

func test_ParallelChunk_MultiChunk(t *testing.T, numCompressors int) {
	data := []byte("Hello, world!")
	chunkSize := int64(5)

	chunker := NewParallelChunker(bytes.NewReader(data), chunkSize, numCompressors, Gzip)
	ch := chunker.Start()

	expectedChunksData := []string{
		"Hello",
		", wor",
		"ld!",
	}

	var receivedChunks []*command.LoadChunkRequest
	for chunk := range ch {
		receivedChunks = append(receivedChunks, chunk)
	}
	if len(receivedChunks) != len(expectedChunksData) {
		t.Fatalf("unexpected number of chunks: got %d, want %d", len(receivedChunks), len(expectedChunksData))
	}

	for i, chunk := range receivedChunks {
		if chunk.SequenceNum != int64(i+1) {
			t.Errorf("unexpected sequence number: got %d, want %d", chunk.SequenceNum, i+1)
		}

		expectedIsLast := i == len(expectedChunksData)-1
		if chunk.IsLast != expectedIsLast {
			t.Errorf("unexpected IsLast value: got %v, want %v", chunk.IsLast, expectedIsLast)
		}

		decompressed := mustGunzip(t, chunk.Data)
		if string(decompressed) != expectedChunksData[i] {
			t.Errorf("unexpected chunk data: got %s, want %s", string(decompressed), expectedChunksData[i])
		}
	}
}

func Test_ParallelChunker_MultiChunk(t *testing.T) {
	t.Run("SingleCompressor", func(t *testing.T) {
		test_ParallelChunk_MultiChunk(t, 1)
	})
	t.Run("MultiCompressor", func(t *testing.T) {
		test_ParallelChunk_MultiChunk(t, 8)
	})
}

func mustGunzip(t *testing.T, data []byte) []byte {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decompressed := new(bytes.Buffer)
	if _, err = io.Copy(decompressed, gzipReader); err != nil {
		t.Fatalf("failed to decompress data: %v", err)
	}
	return decompressed.Bytes()
}
