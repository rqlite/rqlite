package snapshot

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"testing"
)

// Test_V1Encoder_WriteTo tests that the V1Encoder.WriteTo method
// writes a valid Snapshot to the given io.Writer.
func Test_V2Encoder_WriteTo(t *testing.T) {
	testFilePath := makeTempFile(t)
	defer os.Remove(testFilePath)

	// Create V2Encoder with a test file path
	v := NewV2Encoder(testFilePath)

	// Create a buffer to serve as the io.Writer
	buf := new(bytes.Buffer)

	// Write a snapshot to the buffer
	_, err := v.WriteTo(buf)
	if err != nil {
		t.Fatalf("Unexpected error in WriteTo: %v", err)
	}

	// Make a reader from the buffer
	r := bytes.NewReader(buf.Bytes())

	// Now sanity check the snapshot.
	if !ReaderIsV2Snapshot(r) {
		t.Fatalf("ReaderIsV2Snapshot returned false for valid snapshot")
	}

	// Write the Snapshot to a temp file.
	tempSnapshotPath := makeTempFile(t)
	defer os.Remove(tempSnapshotPath)

	if err := os.WriteFile(tempSnapshotPath, buf.Bytes(), 0644); err != nil {
		t.Fatalf("Error writing temp file: %v", err)
	}
	if !FileIsV2Snapshot(tempSnapshotPath) {
		t.Fatalf("FileIsV2Snapshot returned false for valid snapshot")
	}

}

// Test_V1Encoder_WriteToNoFile tests that the V1Encoder.WriteTo method
// returns an error when the given file does not exist.
func Test_V2Encoder_WriteToNoFile(t *testing.T) {
	v := NewV2Encoder("/does/not/exist")
	_, err := v.WriteTo(new(bytes.Buffer))
	if err == nil {
		t.Fatalf("Expected error in WriteTo due to non-existent file, but got nil")
	}
}

func Test_V2SnapshotEncodeDecode(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "test-file")
	if err != nil {
		t.Fatalf("Error creating temp file: %v", err)
	}
	const size = 1024
	_, err = io.CopyN(f, rand.Reader, size)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Encode it as a v2 snapshot to a byte buffer.
	var buf bytes.Buffer
	enc := NewV2Encoder(f.Name())
	n, err := enc.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Check that `n` matches the number of bytes in the buffer.
	if n != int64(buf.Len()) {
		t.Fatalf("expected %d bytes, got %d", n, buf.Len())
	}

	// Pass the byte buffer to a decoder.
	dec := NewV2Decoder(&buf)

	// Have it decode the snapshot to a second byte buffer.
	var decodeBuf bytes.Buffer
	_, err = dec.WriteTo(&decodeBuf)
	if err != nil {
		t.Fatal(err)
	}

	// Check that we get the original contents back.
	f, err = os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var originalBuf bytes.Buffer
	_, err = io.Copy(&originalBuf, f)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(originalBuf.Bytes(), decodeBuf.Bytes()) {
		t.Fatal("original file content and decoded content are not the same")
	}
}

func makeTempFile(t *testing.T) string {
	f, err := os.CreateTemp(t.TempDir(), "test-file")
	if err != nil {
		t.Fatalf("Error creating temp file: %v", err)
	}
	defer f.Close()
	return f.Name()
}
