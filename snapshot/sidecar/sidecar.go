// Package sidecar defines the on-disk format of CRC sidecar files written
// alongside snapshot data files (DB and WAL).
package sidecar

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/rqlite/rqlite/v10/internal/rsum"
)

// Type identifies the checksum algorithm recorded in a sidecar.
type Type string

const (
	// TypeCastagnoli denotes a CRC32 computed with the Castagnoli polynomial.
	TypeCastagnoli Type = "castagnoli"
)

// Sidecar is the on-disk JSON representation of a CRC sidecar file.
//
// CRC is encoded as an 8-character lowercase hex string (e.g. "1a2b3c4d") so
// the file is human-readable and stable across encoders.
type Sidecar struct {
	CRC      string `json:"crc"`
	Type     Type   `json:"type"`
	Disabled bool   `json:"disabled,omitempty"`
}

// NewCastagnoli returns a Sidecar that records sum as a Castagnoli CRC32.
func NewCastagnoli(sum uint32) *Sidecar {
	return &Sidecar{
		CRC:      fmt.Sprintf("%08x", sum),
		Type:     TypeCastagnoli,
		Disabled: false,
	}
}

// CRC32 returns the parsed CRC32 value. It returns an error if the sidecar's
// Type is not a CRC32 algorithm, or if the CRC field is not exactly an
// 8-character hex string.
func (s *Sidecar) CRC32() (uint32, error) {
	if s.Type != TypeCastagnoli {
		return 0, fmt.Errorf("unsupported sidecar type: %q", s.Type)
	}
	if len(s.CRC) != 8 {
		return 0, fmt.Errorf("invalid CRC value %q: expected 8 hex characters, got %d", s.CRC, len(s.CRC))
	}
	sum, err := strconv.ParseUint(s.CRC, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid CRC value %q: %w", s.CRC, err)
	}
	return uint32(sum), nil
}

// WriteFile writes a Castagnoli CRC32 sidecar to path. Always syncs
// the file to disk.
func WriteFile(path string, sum uint32) error {
	b, err := json.Marshal(NewCastagnoli(sum))
	if err != nil {
		return err
	}
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fd.Close() // Ignore error if closed happened earlier.

	if _, err := fd.Write(b); err != nil {
		return err
	}
	if err := fd.Sync(); err != nil {
		return err
	}
	return fd.Close()
}

// ReadFile reads and decodes a sidecar file from path.
func ReadFile(path string) (*Sidecar, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Sidecar
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("invalid sidecar file %s: %w", path, err)
	}
	return &s, nil
}

// ReadCRC32File reads a sidecar from path and returns its CRC32 value.
func ReadCRC32File(path string) (uint32, error) {
	s, err := ReadFile(path)
	if err != nil {
		return 0, err
	}
	return s.CRC32()
}

// CompareFile recomputes the CRC32 of the file at dataPath and compares it
// to the value recorded in the sidecar at sidecarPath.
func CompareFile(dataPath, sidecarPath string) (bool, error) {
	expected, err := ReadCRC32File(sidecarPath)
	if err != nil {
		return false, fmt.Errorf("reading sidecar: %w", err)
	}
	actual, err := rsum.CRC32(dataPath)
	if err != nil {
		return false, fmt.Errorf("calculating CRC32 of data file: %w", err)
	}
	return expected == actual, nil
}
