package chunking

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_SingleChunk(t *testing.T) {
	data := []byte("Hello, World!")
	chunk := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      true,
		Data:        mustCompressData(data),
	}

	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	isLast, err := dechunker.WriteChunk(chunk)
	if err != nil {
		t.Fatalf("failed to write chunk: %v", err)
	}
	if !isLast {
		t.Errorf("WriteChunk did not return true for isLast")
	}

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
	data1 := []byte("Hello, World!")
	chunk1 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        mustCompressData(data1),
	}
	data2 := []byte("I'm OK")
	chunk2 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 2,
		IsLast:      true,
		Data:        mustCompressData(data2),
	}

	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	for _, chunk := range []*proto.LoadChunkRequest{chunk1, chunk2} {
		isLast, err := dechunker.WriteChunk(chunk)
		if err != nil {
			t.Fatalf("failed to write chunk: %v", err)
		}
		if isLast != chunk.IsLast {
			t.Errorf("WriteChunk returned wrong value for isLast")
		}
	}

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

func Test_MultiChunkNilData(t *testing.T) {
	data1 := []byte("Hello, World!")
	chunk1 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        mustCompressData(data1),
	}
	data2 := []byte("I'm OK")
	chunk2 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 2,
		IsLast:      false,
		Data:        mustCompressData(data2),
	}
	chunk3 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 3,
		IsLast:      true,
		Data:        nil,
	}

	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

	for _, chunk := range []*proto.LoadChunkRequest{chunk1, chunk2, chunk3} {
		isLast, err := dechunker.WriteChunk(chunk)
		if err != nil {
			t.Fatalf("failed to write chunk: %v", err)
		}
		if isLast != chunk.IsLast {
			t.Errorf("WriteChunk returned wrong value for isLast")
		}
	}

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
	originalData := []byte("Hello, World!")
	compressedData := mustCompressData(originalData)

	chunk1 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        compressedData,
	}

	chunk2 := &proto.LoadChunkRequest{
		StreamId:    "456",
		SequenceNum: 2,
		IsLast:      true,
		Data:        compressedData,
	}

	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	dechunker, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}

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
	originalData := []byte("Hello, World!")
	compressedData := mustCompressData(originalData)

	chunk1 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 1,
		IsLast:      false,
		Data:        compressedData,
	}

	chunk3 := &proto.LoadChunkRequest{
		StreamId:    "123",
		SequenceNum: 3,
		IsLast:      true,
		Data:        compressedData,
	}

	dir, err := ioutil.TempDir("", "dechunker-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

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

func Test_ReassemblyOfLargeData(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	d, err := NewDechunker(dir)
	if err != nil {
		t.Fatalf("failed to create Dechunker: %v", err)
	}
	defer os.Remove(dir)

	// Create a large random dataset.
	largeData := make([]byte, 2*1024*1024) // 2 MB of data.
	if _, err := rand.Read(largeData); err != nil {
		t.Fatalf("failed to generate large data: %v", err)
	}

	// Split the large data into chunks.
	numChunks := 16
	chunkSize := len(largeData) / numChunks

	// Write the chunks to the Dechunker.
	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize

		isLast := i == numChunks-1
		if _, err := d.WriteChunk(&proto.LoadChunkRequest{
			StreamId:    "1",
			SequenceNum: int64(i + 1),
			IsLast:      isLast,
			Data:        mustCompressData(largeData[start:end]),
		}); err != nil {
			t.Fatalf("failed to write chunk: %v", err)
		}
	}

	outFilePath, err := d.Close()
	if err != nil {
		t.Fatalf("failed to close Dechunker: %v", err)
	}
	defer os.Remove(outFilePath)

	// The output data should be the same as the original largeData.
	outData, err := ioutil.ReadFile(outFilePath)
	if err != nil {
		t.Fatalf("failed to read output file data: %v", err)
	}
	if !bytes.Equal(outData, largeData) {
		t.Fatalf("output file data does not match original data")
	}
}

func Test_CreateDechunkerManager(t *testing.T) {
	dir := os.TempDir()
	manager, err := NewDechunkerManager(dir)
	if err != nil {
		t.Fatalf("failed to create DechunkerManager: %v", err)
	}
	defer manager.Close()
	if manager == nil {
		t.Fatalf("expected DechunkerManager instance, got nil")
	}
	if manager.dir != dir {
		t.Errorf("unexpected dir in manager: expected %s, got %s", dir, manager.dir)
	}
	if manager.m != nil {
		t.Errorf("expected manager map to be nil, got: %v", manager.m)
	}
}

func Test_CreateDechunkerManagerFail(t *testing.T) {
	manager, err := NewDechunkerManager("/does/not/exist/surely")
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if manager != nil {
		t.Fatalf("expected nil manager but got one")
	}
}

func Test_GetDechunker(t *testing.T) {
	dir := os.TempDir()
	manager, err := NewDechunkerManager(dir)
	if err != nil {
		t.Fatalf("failed to create DechunkerManager: %v", err)
	}
	defer manager.Close()

	dechunker1, err := manager.Get("test1")
	if err != nil {
		t.Fatalf("failed to get dechunker: %v", err)
	}
	if dechunker1 == nil {
		t.Fatalf("expected Dechunker instance, got nil")
	}

	// Make sure we get the same dechunker.
	dechunker2, err := manager.Get("test1")
	if err != nil {
		t.Fatalf("failed to get dechunker: %v", err)
	}
	if dechunker2 == nil {
		t.Fatalf("expected Dechunker instance, got nil")
	}
	if dechunker1 != dechunker2 {
		t.Errorf("expected same dechunker instance, got different instances")
	}
}

func Test_DeleteDechunker(t *testing.T) {
	dir := os.TempDir()
	manager, err := NewDechunkerManager(dir)
	if err != nil {
		t.Fatalf("failed to create DechunkerManager: %v", err)
	}
	defer manager.Close()

	dechunker1, err := manager.Get("test1")
	if err != nil {
		t.Fatalf("failed to get dechunker: %v", err)
	}
	if dechunker1 == nil {
		t.Fatalf("expected Dechunker instance, got nil")
	}

	manager.Delete("test1")

	// Attempt to get the Dechunker for the ID "test1"
	dechunker2, err := manager.Get("test1")
	if err != nil {
		t.Fatalf("failed to get dechunker: %v", err)
	}
	if dechunker2 == nil {
		t.Fatalf("expected Dechunker instance, got nil")
	}
	if dechunker1 == dechunker2 {
		t.Errorf("expected different dechunker instances, got same instance")
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
