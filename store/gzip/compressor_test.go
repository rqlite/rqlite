package gzip

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
)

func Test_Compressor_SingleRead(t *testing.T) {
	originalData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	reader := bytes.NewReader(originalData)
	compressor, err := NewCompressor(reader, DefaultBufferSize)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Create a buffer to hold compressed data
	compressedBuffer := make([]byte, DefaultBufferSize)

	n, err := compressor.Read(compressedBuffer)
	if err != nil && err != io.EOF {
		t.Fatalf("Unexpected error while reading: %v", err)
	}

	// Decompress the compressed data
	r, err := gzip.NewReader(bytes.NewReader(compressedBuffer[:n]))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	decompressedBuffer := new(bytes.Buffer)
	_, err = io.Copy(decompressedBuffer, r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	// Verify the decompressed data matches original data
	if !bytes.Equal(decompressedBuffer.Bytes(), originalData) {
		t.Fatalf("Decompressed data does not match original. Got %v, expected %v", decompressedBuffer.Bytes(), originalData)
	}
}

func Test_Compressor_MultipleRead(t *testing.T) {
	originalData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	reader := bytes.NewReader(originalData)
	compressor, err := NewCompressor(reader, DefaultBufferSize)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Create a buffer to hold compressed data
	compressedBuffer := new(bytes.Buffer)

	for {
		_, err := io.CopyN(compressedBuffer, compressor, 8)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Unexpected error while reading: %v", err)
		}
	}

	// Decompress the compressed data
	r, err := gzip.NewReader(bytes.NewReader(compressedBuffer.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	decompressedBuffer := new(bytes.Buffer)
	_, err = io.Copy(decompressedBuffer, r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	// Verify the decompressed data matches original data
	if !bytes.Equal(decompressedBuffer.Bytes(), originalData) {
		t.Fatalf("Decompressed data does not match original. Got %v, expected %v", decompressedBuffer.Bytes(), originalData)
	}
}

func Test_Compressor_MultipleReadSmallBuffer(t *testing.T) {
	originalData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	reader := bytes.NewReader(originalData)
	compressor, err := NewCompressor(reader, 8)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}

	// Create a buffer to hold compressed data
	compressedBuffer := new(bytes.Buffer)

	for {
		_, err := io.CopyN(compressedBuffer, compressor, 32)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Unexpected error while reading: %v", err)
		}
	}

	// Decompress the compressed data
	r, err := gzip.NewReader(bytes.NewReader(compressedBuffer.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	decompressedBuffer := new(bytes.Buffer)
	_, err = io.Copy(decompressedBuffer, r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	// Verify the decompressed data matches original data
	if !bytes.Equal(decompressedBuffer.Bytes(), originalData) {
		t.Fatalf("Decompressed data does not match original. Got %v, expected %v", decompressedBuffer.Bytes(), originalData)
	}
}

func Test_Compressor_CompressFile(t *testing.T) {
	srcFD := mustOpenTempFile(t)
	defer srcFD.Close()
	_, err := io.CopyN(srcFD, bytes.NewReader(bytes.Repeat([]byte("a"), 131072)), 131072)
	if err != nil {
		t.Fatalf("Failed to write to source file: %v", err)
	}
	// Reset file pointer to beginning
	if _, err := srcFD.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	// Compress it.
	compressor, err := NewCompressor(srcFD, DefaultBufferSize)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}
	dstFD := mustOpenTempFile(t)
	defer dstFD.Close()
	_, err = io.Copy(dstFD, compressor)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Decompress it via actual gzip.
	dstUncompressedFD := mustOpenTempFile(t)
	defer dstUncompressedFD.Close()
	dstFD.Seek(0, 0)
	r, err := gzip.NewReader(dstFD)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	_, err = io.Copy(dstUncompressedFD, r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	// Compare the files.
	compareFiles(t, srcFD, dstUncompressedFD)
}

func Test_Compressor_CompressLargeFile(t *testing.T) {
	mb128 := int64(128*1024*1024) + 13
	srcFD := mustOpenTempFile(t)
	_, err := io.CopyN(srcFD, io.LimitReader(rand.New(rand.NewSource(0)), mb128), mb128)
	if err != nil {
		t.Fatalf("Failed to write random data to source file: %v", err)
	}
	defer os.Remove(srcFD.Name())
	defer srcFD.Close()
	// Reset file pointer to beginning
	if _, err := srcFD.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	// Compress it.
	compressor, err := NewCompressor(srcFD, DefaultBufferSize)
	if err != nil {
		t.Fatalf("Failed to create compressor: %v", err)
	}
	dstFD := mustOpenTempFile(t)
	defer os.Remove(dstFD.Name())
	defer dstFD.Close()
	_, err = io.Copy(dstFD, compressor)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Decompress it via actual gzip.
	dstUncompressedFD := mustOpenTempFile(t)
	defer os.Remove(dstUncompressedFD.Name())
	defer dstUncompressedFD.Close()
	dstFD.Seek(0, 0)
	r, err := gzip.NewReader(dstFD)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	_, err = io.Copy(dstUncompressedFD, r)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	// Compare the files.
	compareFileMD5(t, srcFD.Name(), dstUncompressedFD.Name())
}

func mustOpenTempFile(t *testing.T) *os.File {
	t.Helper()
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "compressor-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	return f
}

func compareFiles(t *testing.T, srcFD, dstFD *os.File) {
	t.Helper()
	if _, err := srcFD.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}
	if _, err := dstFD.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	srcFileInfo, err := srcFD.Stat()
	if err != nil {
		t.Fatalf("Failed to stat source file: %v", err)
	}
	dstFileInfo, err := dstFD.Stat()
	if err != nil {
		t.Fatalf("Failed to stat destination file: %v", err)
	}
	if srcFileInfo.Size() != dstFileInfo.Size() {
		t.Fatalf("Source file size (%v) does not match destination file size (%v)", srcFileInfo.Size(), dstFileInfo.Size())
	}

	srcBytes, err := os.ReadFile(srcFD.Name())
	if err != nil {
		t.Fatalf("Failed to read source file: %v", err)
	}
	dstBytes, err := os.ReadFile(dstFD.Name())
	if err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}
	if !bytes.Equal(srcBytes, dstBytes) {
		t.Fatalf("Source data does not match destination data")
	}
}

func compareFileMD5(t *testing.T, srcPath, dstPath string) {
	t.Helper()

	srcMD5, err := md5sum(srcPath)
	if err != nil {
		t.Fatalf("Failed to calculate md5sum of source file: %v", err)
	}
	dstMD5, err := md5sum(dstPath)
	if err != nil {
		t.Fatalf("Failed to calculate md5sum of destination file: %v", err)
	}

	// compare md5sums
	if srcMD5 != dstMD5 {
		t.Fatal("Source file md5sum does not match destination file md5sum")
	}
}

func md5sum(path string) (string, error) {
	// open file
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// create new hash
	h := md5.New()

	// copy file to hash
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to copy file to hash: %w", err)
	}

	// return hex encoded hash
	return hex.EncodeToString(h.Sum(nil)), nil
}
