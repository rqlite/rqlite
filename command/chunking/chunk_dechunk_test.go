package chunking

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"
)

func Test_ChunkingRoundTrip(t *testing.T) {
	data := make([]byte, 1024*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	chunker := NewChunker(bytes.NewReader(data), 1024)

	dechunker, err := NewDechunker(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	// Read chunks from the Chunker and write them to the Dechunker.
	for {
		chunk, err := chunker.Next()
		if err != nil {
			t.Fatalf("failed to read chunk: %v", err)
		}
		done, err := dechunker.WriteChunk(chunk)
		if err != nil {
			t.Fatalf("failed to write chunk: %v", err)
		}
		if chunk.IsLast {
			if !done {
				t.Fatalf("WriteChunk did not return true after writing last chunk")
			}
			break
		}
	}

	outFilePath, err := dechunker.Close()
	if err != nil {
		t.Fatalf("failed to close Dechunker: %v", err)
	}
	defer os.Remove(outFilePath)

	// The output data should be the same as the original data.
	outData, err := os.ReadFile(outFilePath)
	if err != nil {
		t.Fatalf("failed to read output file data: %v", err)
	}
	if !bytes.Equal(outData, data) {
		t.Fatalf("output file data does not match original data")
	}
}
