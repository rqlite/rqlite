package snapshot2

// Stream is a stream of data that can be read from a snapshot.
type Stream struct {
}

// Read reads from the stream.
func (s *Stream) Read(p []byte) (n int, err error) {
	return n, err
}

// Close closes the stream.
func (s *Stream) Close() error {
	return nil
}
