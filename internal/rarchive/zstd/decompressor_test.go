package zstd

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
)

func Test_Decompressor(t *testing.T) {
	testData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")

	// Compress using our Compressor (writes size header + zstd payload).
	compressor, err := NewCompressor(bytes.NewReader(testData), int64(len(testData)), DefaultBufferSize)
	if err != nil {
		t.Fatalf("failed to create compressor: %v", err)
	}
	defer compressor.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, compressor); err != nil {
		t.Fatalf("failed to compress: %v", err)
	}

	// Decompress the data.
	decompressor := NewDecompressor(&buf)
	decompressedBuffer := new(bytes.Buffer)
	_, err = io.Copy(decompressedBuffer, decompressor)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if !bytes.Equal(decompressedBuffer.Bytes(), testData) {
		t.Fatalf("decompressed data does not match original")
	}

	// Check that future reads return io.EOF.
	_, err = decompressor.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	_, err = decompressor.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func Test_Decompressor_UncompressedSize(t *testing.T) {
	testData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")

	compressor, err := NewCompressor(bytes.NewReader(testData), int64(len(testData)), DefaultBufferSize)
	if err != nil {
		t.Fatalf("failed to create compressor: %v", err)
	}
	defer compressor.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, compressor); err != nil {
		t.Fatalf("failed to compress: %v", err)
	}

	// The size is available before any data has been read.
	decompressor := NewDecompressor(&buf)
	sz, err := decompressor.UncompressedSize()
	if err != nil {
		t.Fatalf("failed to get uncompressed size: %v", err)
	}
	if exp, got := int64(len(testData)), sz; exp != got {
		t.Fatalf("expected uncompressed size %d, got %d", exp, got)
	}

	// Reading the size does not disturb the data.
	decompressed, err := io.ReadAll(decompressor)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}
	if !bytes.Equal(decompressed, testData) {
		t.Fatalf("decompressed data does not match original")
	}

	// The size is still available once the stream has been read.
	sz, err = decompressor.UncompressedSize()
	if err != nil {
		t.Fatalf("failed to get uncompressed size after read: %v", err)
	}
	if exp, got := int64(len(testData)), sz; exp != got {
		t.Fatalf("expected uncompressed size %d after read, got %d", exp, got)
	}

	// A stream too short to hold the header is an error.
	_, err = NewDecompressor(bytes.NewReader([]byte{1, 2, 3})).UncompressedSize()
	if err == nil {
		t.Fatalf("expected error reading size from truncated header")
	}
}

func Test_Decompressor_EndToEnd(t *testing.T) {
	ln := mustListenTCP()
	defer ln.Close()

	testData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")

	// Accept connections on the listener.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				t.Errorf("failed to accept connection: %v", err)
			}
			compressor, err := NewCompressor(bytes.NewBuffer(testData), int64(len(testData)), DefaultBufferSize)
			if err != nil {
				t.Errorf("failed to create compressor: %v", err)
				conn.Close()
				continue
			}
			if _, err := io.Copy(conn, compressor); err != nil {
				t.Errorf("failed to copy data: %v", err)
			}
			compressor.Close()
			conn.Close()
		}
	}()

	// Connect to the listener.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to listener: %v", err)
	}
	defer conn.Close()

	// Decompress the data.
	decompressor := NewDecompressor(conn)
	dstBuf := new(bytes.Buffer)
	_, err = io.Copy(dstBuf, decompressor)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if !bytes.Equal(dstBuf.Bytes(), testData) {
		t.Fatalf("decompressed data does not match original")
	}
}

func mustListenTCP() net.Listener {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	return ln
}
