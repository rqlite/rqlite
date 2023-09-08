package gzip

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net"
	"testing"
)

func Test_Decompressor(t *testing.T) {
	// Write some gzipped data to a buffer
	testData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	gzw.Write([]byte(testData))
	gzw.Close()

	// Decompress the data
	decompressor := NewDecompressor(&buf)
	decompressedBuffer := new(bytes.Buffer)
	_, err := io.Copy(decompressedBuffer, decompressor)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	// Verify the decompressed data matches original data
	if !bytes.Equal(decompressedBuffer.Bytes(), []byte(testData)) {
		t.Fatalf("decompressed data does not match original")
	}
}

func Test_Decompressor_EndToEnd(t *testing.T) {
	ln := mustListenTCP()
	defer ln.Close()

	testData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	srcBuf := bytes.NewBuffer(testData)

	// Accept connections on the listern
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				t.Errorf("failed to accept connection: %v", err)
			}
			compressor := NewCompressor(srcBuf, DefaultBufferSize)
			if _, err := io.Copy(conn, compressor); err != nil {
				t.Errorf("failed to copy data: %v", err)
			}
			conn.Close()
		}
	}()

	// Connect to the listener
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect to listener: %v", err)
	}
	defer conn.Close()

	// Decompress the data
	decompressor := NewDecompressor(conn)
	dstBuf := new(bytes.Buffer)
	_, err = io.Copy(dstBuf, decompressor)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if !bytes.Equal(dstBuf.Bytes(), []byte(testData)) {
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
