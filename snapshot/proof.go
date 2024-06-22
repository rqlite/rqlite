package snapshot

import (
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"time"
)

// Proof represents the key attributes of the source data used to create
// a ReferentialSnapshot.
type Proof struct {
	SizeBytes        int64     `json:"size_bytes"`
	LastModifiedTime time.Time `json:"last_modified_time"`
	CRC32            uint32    `json:"crc"`
}

// NewProof returns a new Proof.
func NewProof(size int64, lmt time.Time, crc uint32) *Proof {
	return &Proof{
		SizeBytes:        size,
		LastModifiedTime: lmt,
		CRC32:            crc,
	}
}

// NewProofFromFile returns a new Proof from the file at the given path.
func NewProofFromFile(path string) (*Proof, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	sum, err := crc(path)
	if err != nil {
		return nil, err
	}
	return NewProof(fi.Size(), fi.ModTime(), sum), nil
}

// String returns a string representation of the Proof.
func (p *Proof) String() string {
	return "Proof{SizeBytes: " + strconv.FormatInt(p.SizeBytes, 10) +
		", LastModifiedTime: " +
		p.LastModifiedTime.String() + ", CRC32: " +
		strconv.FormatUint(uint64(p.CRC32), 10) + "}"
}

// Equals returns true if the two Proofs are equal.
func (p *Proof) Equals(o *Proof) bool {
	return p.SizeBytes == o.SizeBytes && p.LastModifiedTime.Equal(o.LastModifiedTime) && p.CRC32 == o.CRC32
}

// Marshal returns the JSON encoding of the Proof.
func (p *Proof) Marshal() ([]byte, error) {
	return json.Marshal(p)
}

// UnmarshalProof returns a new Proof from the JSON encoding.
func UnmarshalProof(data []byte) (*Proof, error) {
	p := &Proof{}
	if err := json.Unmarshal(data, p); err != nil {
		return nil, err
	}
	return p, nil
}

func crc(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
