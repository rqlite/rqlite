package streamer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func Test_Encoder(t *testing.T) {
	testStreamer := func(bufferSize int) {
		fileContent := []string{
			"this is content for file1",
			"this is more content, which is for file 2",
			"finally, we've got a third file here",
		}
		filePaths := make([]string, len(fileContent))

		for i, content := range fileContent {
			file := makeTempFile()
			defer os.Remove(file)

			filePaths[i] = file
			if err := os.WriteFile(file, []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}
		encoder := NewEncoder(filePaths)
		if bufferSize > 0 {
			encoder.SetBufferSize(bufferSize)
		}

		buf := bytes.NewBuffer(nil)
		gzipReader, err := gzip.NewReader(encoder)
		if err != nil {
			t.Fatal(err)
		}
		reader := tar.NewReader(gzipReader)
		for i := 0; i < len(fileContent); i++ {
			header, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}

			buf.Reset()
			_, err = io.Copy(buf, reader)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(buf.Bytes(), []byte(fileContent[i])) {
				t.Errorf("Content of file %s does not match expected content", header.Name)
			}
		}

		if _, err := reader.Next(); err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}

		if err := encoder.Close(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("default buffer size", func(t *testing.T) {
		testStreamer(0)
	})
	t.Run("buffer size 1", func(t *testing.T) {
		testStreamer(1)
	})
	t.Run("buffer size 4", func(t *testing.T) {
		testStreamer(4)
	})
}

func Test_EncoderDecoder(t *testing.T) {
	// Create temporary files for testing
	files := []string{makeTempFile(), makeTempFile(), makeTempFile()}
	contents := []string{"Content of file 0", "Content of file 1", "Content of file 2"}
	for i, file := range files {
		mustWriteFile(file, []byte(contents[i]))
		defer os.Remove(file)
	}

	// Create Encoder and write files to it
	encoder := NewEncoder(files)
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, encoder); err != nil {
		t.Fatal(err)
	}

	// Create Decoder and read files from it
	decoder, err := NewDecoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(files); i++ {
		header, err := decoder.Next()
		if err != nil {
			t.Fatal(err)
		}

		if exp, got := filepath.Base(files[i]), header.Name; exp != got {
			t.Errorf("expected name %s, got %s", exp, got)
		}

		if exp, got := int64(len(contents[i])), header.Size; exp != got {
			t.Errorf("expected size %d, got %d", exp, got)
		}

		data := make([]byte, header.Size)
		if _, err := io.ReadFull(decoder, data); err != nil {
			t.Fatal(err)
		}

		if exp, got := contents[i], string(data); exp != got {
			t.Errorf("expected content %s, got %s", exp, got)
		}
	}

	// Ensure no more files
	if _, err := decoder.Next(); err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}

	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustWriteFile(filename string, data []byte) {
	if err := os.WriteFile(filename, data, 0644); err != nil {
		panic(err)
	}
}

func makeTempFile() string {
	fd, err := os.CreateTemp("", "streamer-encoder-testfile")
	if err != nil {
		panic(err)
	}
	fd.Close()
	return fd.Name()
}
