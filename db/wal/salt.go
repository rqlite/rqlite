package wal

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Salt represents the two 32-bit salt values from a SQLite WAL header.
type Salt [2]uint32

// Equal reports whether s and other hold the same salt values.
func (s Salt) Equal(other Salt) bool {
	return s == other
}

// String returns a human-readable representation of s.
func (s Salt) String() string {
	return fmt.Sprintf("Salt(%d,%d)", s[0], s[1])
}

// ReadSaltAt reads the salt values from the WAL header via the given ReaderAt.
func ReadSaltAt(r io.ReaderAt) (Salt, error) {
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, 16); err != nil {
		return Salt{}, err
	}
	return Salt{
		binary.BigEndian.Uint32(buf[0:]),
		binary.BigEndian.Uint32(buf[4:]),
	}, nil
}
