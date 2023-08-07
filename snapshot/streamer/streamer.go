package streamer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
)

const (
	defaultBufferSize = 16384
)

// Encoder is a io.ReadCloser that streams a gzipped tar archive of the given files.
type Encoder struct {
	files   []string // List of files to send
	current *os.File // Current file being read
	tarW    *tar.Writer
	gzipW   *gzip.Writer
	buf     bytes.Buffer

	bufferSize int
}

// New returns a new Encoder.
func NewEncoder(files []string) *Encoder {
	s := &Encoder{
		files:      files,
		bufferSize: defaultBufferSize,
	}
	s.gzipW = gzip.NewWriter(&s.buf)
	s.tarW = tar.NewWriter(s.gzipW)
	return s
}

// SetBufferSize sets the buffer size.
func (s *Encoder) SetBufferSize(bufferSize int) {
	s.bufferSize = bufferSize
}

// Read reads from the Encoder.
func (s *Encoder) Read(p []byte) (int, error) {
	for {
		// If there's data in the buffer, read it
		if s.buf.Len() > 0 {
			return s.buf.Read(p)
		}

		// If all files are processed, return EOF
		if len(s.files) == 0 && s.current == nil {
			if s.tarW != nil {
				if err := s.tarW.Close(); err != nil {
					return 0, err
				}
				s.tarW = nil
				if err := s.gzipW.Close(); err != nil {
					return 0, err
				}
				continue // Make sure we return the tar footer.
			}
			return 0, io.EOF
		}

		// If we're between files, open the next one
		if s.current == nil {
			file, err := os.Open(s.files[0])
			if err != nil {
				return 0, err
			}

			s.current = file
			s.files = s.files[1:]
			fileInfo, err := file.Stat()
			if err != nil {
				return 0, err
			}

			header := &tar.Header{
				Name: fileInfo.Name(),
				Mode: int64(fileInfo.Mode()),
				Size: fileInfo.Size(),
			}

			if err := s.tarW.WriteHeader(header); err != nil {
				return 0, err
			}
		}

		// Copy up to a chunk of file data into the tar writer
		_, err := io.CopyN(s.tarW, s.current, int64(s.bufferSize))
		if err == io.EOF {
			s.current.Close()
			s.current = nil
		} else if err != nil {
			return 0, err
		}
	}
}

// Close closes the Encoder.
func (s *Encoder) Close() error {
	if s.tarW != nil {
		if err := s.tarW.Close(); err != nil {
			return err
		}
	}
	if err := s.gzipW.Close(); err != nil {
		return err
	}
	if s.current != nil {
		if err := s.current.Close(); err != nil {
			return err
		}
	}
	return nil
}
