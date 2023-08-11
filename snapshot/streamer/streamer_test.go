package streamer

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"google.golang.org/protobuf/proto"
)

func Test_EncoderOpenCloseOK(t *testing.T) {
	file1 := makeTempFile()
	mustWriteFile(file1, []byte("content1"))
	file2 := makeTempFile()
	mustWriteFile(file2, []byte("content2"))

	encoder := NewEncoder()
	if err := encoder.Open(file1, file2); err != nil {
		t.Fatalf("Failed to open files in encoder: %v", err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("Failed to close decoder: %v", err)
	}
}

func Test_EncoderOpenFail(t *testing.T) {
	encoder := NewEncoder()
	err := encoder.Open("/non/existent/file")
	if err == nil {
		t.Fatalf("Expected error when opening non-existent file, got nil")
	}
}

// Test_EncoderFullReadOK tests that reading works correctly.
func Test_EncoderFullReadOK(t *testing.T) {
	file1 := makeTempFile()
	mustWriteFile(file1, []byte("content1"))
	file2 := makeTempFile()
	mustWriteFile(file2, []byte("content234"))

	encoder := NewEncoder()
	if err := encoder.Open(file1, file2); err != nil {
		t.Fatalf("Failed to open files in encoder: %v", err)
	}
	expN, err := encoder.EncodedSize()
	if err != nil {
		t.Fatalf("Failed to get encoded size: %v", err)
	}
	totalN := int64(0)

	// Read the header
	hdrLenBuf := make([]byte, 4)
	if n, err := io.ReadFull(encoder, hdrLenBuf); err != nil {
		t.Fatalf("Failed to read header length: %v", err)
	} else {
		totalN += int64(n)
	}
	hdrLen := binary.BigEndian.Uint32(hdrLenBuf)
	hdrBuf := make([]byte, hdrLen)
	if n, err := io.ReadFull(encoder, hdrBuf); err != nil {
		t.Fatalf("Failed to read header: %v", err)
	} else {
		totalN += int64(n)
	}

	hdr := &Header{}
	if err := proto.Unmarshal(hdrBuf, hdr); err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}
	if len(hdr.Files) != 2 {
		t.Fatalf("Expected 2 files in header, got %d", len(hdr.Files))
	}

	// Check the files
	file1Len := hdr.Files[0].Size
	file1Buf := new(bytes.Buffer)
	if n, err := io.CopyN(file1Buf, encoder, file1Len); err != nil {
		t.Fatalf("Failed to read file 1: %v", err)
	} else {
		totalN += n
	}
	if exp, got := "content1", file1Buf.String(); exp != got {
		t.Fatalf("Expected file 1 contents to be '%s', got '%s'", exp, got)
	}

	file2Len := hdr.Files[1].Size
	file2Buf := new(bytes.Buffer)
	if n, err := io.CopyN(file2Buf, encoder, file2Len); err != nil {
		t.Fatalf("Failed to read file 1: %v", err)
	} else {
		totalN += n
	}
	if exp, got := "content234", file2Buf.String(); exp != got {
		t.Fatalf("Expected file 2 contents to be '%s', got '%s'", exp, got)
	}

	// Check the total number of bytes read
	if totalN != expN {
		t.Fatalf("Expected to read %d bytes, got %d", expN, totalN)
	}

	if err := encoder.Close(); err != nil {
		t.Fatalf("Failed to close decoder: %v", err)
	}
}

// Test_EncoderChunkedReadOK tests that reading works correctly,
// even when partial are reads are done.
func Test_EncoderChunkedReadOK(t *testing.T) {
	file1 := makeTempFile()
	mustWriteFile(file1, []byte("content1"))
	file2 := makeTempFile()
	mustWriteFile(file2, []byte("content234"))

	encoder := NewEncoder()
	if err := encoder.Open(file1, file2); err != nil {
		t.Fatalf("Failed to open files in encoder: %v", err)
	}
	expN, err := encoder.EncodedSize()
	if err != nil {
		t.Fatalf("Failed to get encoded size: %v", err)
	}

	var actualOutput []byte
	buf := make([]byte, 10)
	for {
		n, err := encoder.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read from encoder: %v", err)
		}
		actualOutput = append(actualOutput, buf[:n]...)
		if err == io.EOF {
			break
		}
	}
	if exp, got := expN, int64(len(actualOutput)); exp != got {
		t.Fatalf("Expected to read %d bytes, got %d", exp, got)
	}

	hdrLen := binary.BigEndian.Uint32(actualOutput[:4])
	hdr := &Header{}
	if err := proto.Unmarshal(actualOutput[4:hdrLen+4], hdr); err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}
	if len(hdr.Files) != 2 {
		t.Fatalf("Expected 2 files in header, got %d", len(hdr.Files))
	}
	start := int64(hdrLen + 4)
	end := start + int64(hdr.Files[0].Size)
	if string(actualOutput[start:end]) != "content1" {
		t.Fatalf("file 1 contents incorrect")
	}
	start = end
	end = start + int64(hdr.Files[1].Size)
	if string(actualOutput[start:end]) != "content234" {
		t.Fatalf("file 2 contents incorrect")
	}
}

func Test_EncoderDecoder_ReadAllOK(t *testing.T) {
	// Create temporary files for testing
	files := []string{makeTempFile(), makeTempFile(), makeTempFile()}
	contents := []string{
		"X the content of file 0000",
		"Content of file 1",
		"Content of file 222222222",
	}
	for i, file := range files {
		mustWriteFile(file, []byte(contents[i]))
		defer os.Remove(file)
	}

	// Create Encoder and write files to it
	encoder := NewEncoder()
	if err := encoder.Open(files...); err != nil {
		t.Fatalf("Failed to open encoder: %v", err)
	}

	// Create Decoder and read files from it
	decoder := NewDecoder(encoder)
	if err := decoder.Open(); err != nil {
		t.Fatalf("Failed to open decoder: %v", err)
	}

	for i := range files {
		f, err := decoder.Next()
		if err != nil {
			t.Fatalf("Failed to get next file: %v", err)
		}
		if f.Size != int64(len(contents[i])) {
			t.Fatalf("Expected file size to be %d, got %d", len(contents[i]), f.Size)
		}
		buf := make([]byte, f.Size)
		if _, err := io.ReadFull(decoder, buf); err != nil {
			t.Fatalf("Failed to read file contents: %v", err)
		}
		if exp, got := contents[i], string(buf); exp != got {
			t.Fatalf("Expected file contents to be '%s', got '%s'", exp, got)
		}

		// Confirm further reads return EOF
		if _, err := decoder.Read(buf); err != io.EOF {
			t.Fatalf("Expected io.EOF, got %v", err)
		}
	}

	// There should be no more files.
	if _, err := decoder.Next(); err != io.EOF {
		t.Fatalf("Expected io.EOF, got %v", err)
	}

	if err := decoder.Close(); err != nil {
		t.Fatalf("Failed to close decoder: %v", err)
	}
}

// Test that Next() discards data.

func mustWriteFile(filename string, data []byte) {
	if err := os.WriteFile(filename, data, 0644); err != nil {
		panic(err)
	}
}

func makeTempFile() string {
	fd, err := os.CreateTemp("", "streamer-encoder-testfile-*")
	if err != nil {
		panic(err)
	}
	fd.Close()
	return fd.Name()
}
