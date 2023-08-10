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

// package streamer

// import (
// 	"archive/tar"
// 	"bytes"
// 	"compress/gzip"
// 	"io"
// 	"os"
// 	"testing"
// )

// func Test_Encoder(t *testing.T) {
// 	testStreamer := func(bufferSize int) {
// 		fileContent := []string{
// 			"this is content for file1",
// 			"this is more content, which is for file 2",
// 			"finally, we've got a third file here",
// 		}
// 		filePaths := make([]string, len(fileContent))

// 		for i, content := range fileContent {
// 			file := makeTempFile()
// 			defer os.Remove(file)

// 			filePaths[i] = file
// 			if err := os.WriteFile(file, []byte(content), 0644); err != nil {
// 				t.Fatal(err)
// 			}
// 		}
// 		encoder := NewEncoder(filePaths)
// 		if bufferSize > 0 {
// 			encoder.SetBufferSize(bufferSize)
// 		}

// 		buf := bytes.NewBuffer(nil)
// 		gzipReader, err := gzip.NewReader(encoder)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		reader := tar.NewReader(gzipReader)
// 		for i := 0; i < len(fileContent); i++ {
// 			header, err := reader.Next()
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			buf.Reset()
// 			_, err = io.Copy(buf, reader)
// 			if err != nil {
// 				t.Fatal(err)
// 			}

// 			if !bytes.Equal(buf.Bytes(), []byte(fileContent[i])) {
// 				t.Errorf("Content of file %s does not match expected content", header.Name)
// 			}
// 		}

// 		if _, err := reader.Next(); err != io.EOF {
// 			t.Errorf("Expected EOF, got %v", err)
// 		}

// 		if err := encoder.Close(); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	t.Run("default buffer size", func(t *testing.T) {
// 		testStreamer(0)
// 	})
// 	t.Run("buffer size 1", func(t *testing.T) {
// 		testStreamer(1)
// 	})
// 	t.Run("buffer size 4", func(t *testing.T) {
// 		testStreamer(4)
// 	})
// }

// func Test_EncoderDecoder(t *testing.T) {
// 	// Create temporary files for testing
// 	files := []string{makeTempFile(), makeTempFile(), makeTempFile()}
// 	contents := []string{"Content of file 0", "Content of file 1", "Content of file 2"}
// 	for i, file := range files {
// 		mustWriteFile(file, []byte(contents[i]))
// 		defer os.Remove(file)
// 	}

// 	// Create Encoder and write files to it
// 	encoder := NewEncoder(files)
// 	var buf bytes.Buffer
// 	if _, err := io.Copy(&buf, encoder); err != nil {
// 		t.Fatal(err)
// 	}

// 	// Create Decoder and read files from it
// 	decoder, err := NewDecoder(&buf)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	for i := 0; i < len(files); i++ {
// 		header, err := decoder.Next()
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		if exp, got := filepath.Base(files[i]), header.Name; exp != got {
// 			t.Errorf("expected name %s, got %s", exp, got)
// 		}

// 		if exp, got := int64(len(contents[i])), header.Size; exp != got {
// 			t.Errorf("expected size %d, got %d", exp, got)
// 		}

// 		data := make([]byte, header.Size)
// 		if _, err := io.ReadFull(decoder, data); err != nil {
// 			t.Fatal(err)
// 		}

// 		if exp, got := contents[i], string(data); exp != got {
// 			t.Errorf("expected content %s, got %s", exp, got)
// 		}
// 	}

// 	// Ensure no more files
// 	if _, err := decoder.Next(); err != io.EOF {
// 		t.Errorf("expected EOF, got %v", err)
// 	}

// 	if err := decoder.Close(); err != nil {
// 		t.Fatal(err)
// 	}
// }

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
