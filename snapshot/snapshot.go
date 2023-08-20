package snapshot

import (
	"io"
)

type Snapshot struct {
	walData []byte
	files   []string
}

func NewWALSnapshot(b []byte) *Snapshot {
	return &Snapshot{
		walData: b,
	}
}

func NewFullSnapshot(files ...string) *Snapshot {
	return &Snapshot{
		files: files,
	}
}

func (s *Snapshot) Persist(sink io.Writer) error {
	stream, err := s.OpenStream()
	if err != nil {
		return err
	}
	defer stream.Close()

	_, err = io.Copy(sink, stream)
	return err
}

func (s *Snapshot) Release() error {
	return nil
}

func (s *Snapshot) OpenStream() (*Stream, error) {
	if len(s.files) > 0 {
		return NewFullStream(s.files...)
	}
	return NewIncrementalStream(s.walData)
}
