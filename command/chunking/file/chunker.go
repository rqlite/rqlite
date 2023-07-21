package file

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/golang/snappy"
)

// create an enum for compression, none, gzip, snappy
type Compression int

const (
	None   Compression = iota
	Gzip               = 1
	Snappy             = 2
)

func ProcessChunks(reader io.Reader, chunkSize int64, compression Compression) (string, error) {
	dirPath, err := os.MkdirTemp("", "chunking")
	if err != nil {
		return "", err
	}

	chunkIndex := 0
	for {
		outFile, err := os.Create(filepath.Join(dirPath, strconv.Itoa(chunkIndex)))
		if err != nil {
			return "", err
		}

		var writer io.Writer = outFile

		switch compression {
		case None:
			writer = outFile
		case Gzip:
			gzipWriter := gzip.NewWriter(outFile)
			defer gzipWriter.Close()
			writer = gzipWriter
		case Snappy:
			snappyWriter := snappy.NewBufferedWriter(outFile)
			defer snappyWriter.Close()
			writer = snappyWriter
		}

		n, err := io.CopyN(writer, reader, chunkSize)
		outFile.Close()
		if err == io.EOF {
			return dirPath, nil
		}
		if err != nil {
			return "", err
		}
		if n < chunkSize {
			return dirPath, nil
		}

		chunkIndex++
	}
}
