package chunking

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/command"
)

func Test_SingleChunk(t *testing.T) {
	// Define the chunk data.
	data := []byte("Hello, World!")
	chunk := &command.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      true,
		Data:        mustCompressData(data),
	}

	// Create a temporary directory for testing.
	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir) // Clean up after the test is done.

	// Create the Dechunker.
	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	// Write the chunk to the Dechunker.
	isLast, err := dechunker.WriteChunk(chunk)
	if err != nil {
		t.Fatalf("failed to write chunk: %v", err)
	}
	if !isLast {
		t.Errorf("WriteChunk did not return true for isLast")
	}

	// Close the Dechunker.
	filePath, err := dechunker.Close()
	if err != nil {
		t.Fatalf("failed to close Dechunker: %v", err)
	}

	// Check the contents of the output file.
	got, err := ioutil.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("output file data = %q; want %q", got, data)
	}
}

func Test_MultiChunk(t *testing.T) {
	// Define the chunked data.
	data1 := []byte("Hello, World!")
	chunk1 := &command.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        mustCompressData(data1),
	}
	data2 := []byte("I'm OK")
	chunk2 := &command.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 2,
		IsLast:      true,
		Data:        mustCompressData(data2),
	}

	// Create a temporary directory for testing.
	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir) // Clean up after the test is done.

	// Create the Dechunker.
	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	// Write the chunk to the Dechunker.
	isLast, err := dechunker.WriteChunk(chunk1)
	if err != nil {
		t.Fatalf("failed to write chunk: %v", err)
	}
	if isLast {
		t.Errorf("WriteChunk returned true for isLast")
	}

	// Write the chunk to the Dechunker.
	isLast, err = dechunker.WriteChunk(chunk2)
	if err != nil {
		t.Fatalf("failed to write chunk: %v", err)
	}
	if !isLast {
		t.Errorf("WriteChunk did not return true for isLast")
	}

	// Close the Dechunker.
	filePath, err := dechunker.Close()
	if err != nil {
		t.Fatalf("failed to close Dechunker: %v", err)
	}

	// Check the contents of the output file.
	got, err := ioutil.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if string(got) != string(data1)+string(data2) {
		t.Errorf("output file data = %q; want %q", got, string(data1)+string(data2))
	}
}

func Test_UnexpectedStreamID(t *testing.T) {
	// Define the original and compressed chunk data.
	originalData := []byte("Hello, World!")
	compressedData := mustCompressData(originalData)

	chunk1 := &command.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        compressedData,
	}

	chunk2 := &command.LoadChunkRequest{
		StreamId:    "456",
		SequenceNum: 2,
		IsLast:      true,
		Data:        compressedData,
	}

	// Create a temporary directory for testing.
	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir) // Clean up after the test is done.

	// Create the Dechunker.
	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	// Write the first chunk to the Dechunker.
	_, err = dechunker.WriteChunk(chunk1)
	if err != nil {
		t.Fatalf("failed to write first chunk: %v", err)
	}

	// Write the second chunk with a different stream ID, which should result in an error.
	_, err = dechunker.WriteChunk(chunk2)
	if err == nil {
		t.Fatalf("expected error when writing chunk with different stream ID, but got nil")
	}
	if !strings.Contains(err.Error(), "unexpected stream ID") {
		t.Errorf("error = %v; want an error about unexpected stream ID", err)
	}
}

func Test_ChunksOutOfOrder(t *testing.T) {
	// Define the original and compressed chunk data.
	originalData := []byte("Hello, World!")
	compressedData := mustCompressData(originalData)

	chunk1 := &command.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        compressedData,
	}

	chunk3 := &command.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 3,
		IsLast:      true,
		Data:        compressedData,
	}

	// Create a temporary directory for testing.
	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir) // Clean up after the test is done.

	// Create the Dechunker.
	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	// Write the first chunk to the Dechunker.
	_, err = dechunker.WriteChunk(chunk1)
	if err != nil {
		t.Fatalf("failed to write first chunk: %v", err)
	}

	// Write the third chunk (skipping the second), which should result in an error.
	_, err = dechunker.WriteChunk(chunk3)
	if err == nil {
		t.Fatalf("expected error when writing chunks out of order, but got nil")
	}
	if !strings.Contains(err.Error(), "chunks received out of order") {
		t.Errorf("error = %v; want an error about chunks received out of order", err)
	}
}

func mustCompressData(data []byte) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(data); err != nil {
		panic(fmt.Sprintf("failed to write compressed data: %v", err))
	}

	if err := gz.Close(); err != nil {
		panic(fmt.Sprintf("failed to close gzip writer: %v", err))
	}

	return buf.Bytes()
}
