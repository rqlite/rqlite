package snapshot9

import (
	"encoding/json"
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
