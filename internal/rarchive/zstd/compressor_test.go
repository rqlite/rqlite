package zstd

import (
	"bytes"
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
	compressor := NewCompressor(bytes.NewReader(originalData))

	// Read all compressed output (length prefix + payload)
	compressed, err := io.ReadAll(compressor)
	if err != nil {
		t.Fatalf("Unexpected error while reading: %v", err)
	}

	// Decompress using our Decompressor
	decompressor := NewDecompressor(bytes.NewReader(compressed))
	decompressed, err := io.ReadAll(decompressor)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	if !bytes.Equal(decompressed, originalData) {
		t.Fatalf("Decompressed data does not match original")
	}
}

func Test_Compressor_MultipleRead(t *testing.T) {
	originalData := []byte("This is a test string, xxxxx -- xxxxxx -- test should compress")
	compressor := NewCompressor(bytes.NewReader(originalData))

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

	decompressor := NewDecompressor(bytes.NewReader(compressedBuffer.Bytes()))
	decompressed, err := io.ReadAll(decompressor)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	if !bytes.Equal(decompressed, originalData) {
		t.Fatalf("Decompressed data does not match original")
	}
}

func Test_Compressor_CompressFile(t *testing.T) {
	srcFD := mustOpenTempFile(t)
	defer srcFD.Close()
	_, err := io.CopyN(srcFD, bytes.NewReader(bytes.Repeat([]byte("a"), 131072)), 131072)
	if err != nil {
		t.Fatalf("Failed to write to source file: %v", err)
	}
	if _, err := srcFD.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	// Compress it.
	compressor := NewCompressor(srcFD)
	dstFD := mustOpenTempFile(t)
	defer dstFD.Close()
	_, err = io.Copy(dstFD, compressor)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Decompress it using our Decompressor.
	dstUncompressedFD := mustOpenTempFile(t)
	defer dstUncompressedFD.Close()
	dstFD.Seek(0, 0)
	decompressor := NewDecompressor(dstFD)
	_, err = io.Copy(dstUncompressedFD, decompressor)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

	compareFiles(t, srcFD, dstUncompressedFD)
}

func Test_Compressor_CompressLargeFile(t *testing.T) {
	mb64 := int64(64*1024*1024) + 13
	srcFD := mustOpenTempFile(t)
	_, err := io.CopyN(srcFD, io.LimitReader(rand.New(rand.NewSource(0)), mb64), mb64)
	if err != nil {
		t.Fatalf("Failed to write random data to source file: %v", err)
	}
	defer os.Remove(srcFD.Name())
	defer srcFD.Close()
	if _, err := srcFD.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek to beginning of file: %v", err)
	}

	// Compress it.
	compressor := NewCompressor(srcFD)
	dstFD := mustOpenTempFile(t)
	defer os.Remove(dstFD.Name())
	defer dstFD.Close()
	_, err = io.Copy(dstFD, compressor)
	if err != nil {
		t.Fatalf("Failed to compress: %v", err)
	}

	// Decompress it using our Decompressor.
	dstUncompressedFD := mustOpenTempFile(t)
	defer os.Remove(dstUncompressedFD.Name())
	defer dstUncompressedFD.Close()
	dstFD.Seek(0, 0)
	decompressor := NewDecompressor(dstFD)
	_, err = io.Copy(dstUncompressedFD, decompressor)
	if err != nil {
		t.Fatalf("Failed to decompress: %v", err)
	}

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

	if srcMD5 != dstMD5 {
		t.Fatal("Source file md5sum does not match destination file md5sum")
	}
}

func md5sum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to copy file to hash: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
