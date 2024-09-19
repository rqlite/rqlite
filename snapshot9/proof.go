package snapshot9

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
)

// Proof represents the key attributes of the source data used to create
// a ReferentialSnapshot.
type Proof struct {
	SizeBytes        int64     `json:"size_bytes"`
	LastModifiedTime time.Time `json:"last_modified_time"`
}

// NewProof returns a new Proof.
func NewProof(size int64, lmt time.Time) *Proof {
	return &Proof{
		SizeBytes:        size,
		LastModifiedTime: lmt,
	}
}

// NewProofFromFile returns a new Proof from the file at the given path.
func NewProofFromFile(path string) (*Proof, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return NewProof(fi.Size(), fi.ModTime()), nil
}

// String returns a string representation of the Proof.
func (p *Proof) String() string {
	return "Proof{SizeBytes: " + strconv.FormatInt(p.SizeBytes, 10) +
		", LastModifiedTime: " + p.LastModifiedTime.String() + "}"
}

// Equals returns true if the two Proofs are equal.
func (p *Proof) Equals(o *Proof) bool {
	if o == nil {
		return false
	}
	return p.SizeBytes == o.SizeBytes && p.LastModifiedTime.Equal(o.LastModifiedTime)
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
