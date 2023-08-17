package snapshot

import "io"

type Snapshot struct {
}

func New() *Snapshot {
	return &Snapshot{}
}

func (s *Snapshot) Persist(sink *Sink) error {
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
	return nil, nil
}
