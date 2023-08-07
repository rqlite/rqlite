package streamer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
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
		streamer := New(filePaths)
		if bufferSize > 0 {
			streamer.SetBufferSize(bufferSize)
		}

		buf := bytes.NewBuffer(nil)
		gzipReader, err := gzip.NewReader(streamer)
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

		// Close the Streamer
		if err := streamer.Close(); err != nil {
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

func makeTempFile() string {
	fd, err := os.CreateTemp("", "streamer-encoder-testfile")
	if err != nil {
		panic(err)
	}
	fd.Close()
	return fd.Name()
}
