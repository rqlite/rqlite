package command

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
)

const (
	DefaultBatchThreshold = 5
	DefaultSizeThreshold  = 100
)

type Requester interface {
	proto.Message
	GetRequest() *Request
}

type RequestMarshaler struct {
	BatchThreshold int
	SizeThreshold  int
}

func NewRequestMarshaler() *RequestMarshaler {
	return &RequestMarshaler{
		BatchThreshold: DefaultBatchThreshold,
		SizeThreshold:  DefaultSizeThreshold,
	}
}

func (m *RequestMarshaler) Marshal(r Requester) ([]byte, bool, error) {
	compress := false

	stmts := r.GetRequest().GetStatements()
	if len(stmts) >= m.BatchThreshold {
		compress = true
	} else {
		for i := range stmts {
			if len(stmts[i].Sql) >= m.SizeThreshold {
				compress = true
				break
			}
		}
	}

	b, err := proto.Marshal(r)
	if err != nil {
		return nil, false, err
	}

	if compress {
		var buf bytes.Buffer
		gz, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		if err != nil {
			return nil, false, err
		}
		if _, err := gz.Write(b); err != nil {
			return nil, false, err
		}
		if err := gz.Close(); err != nil {
			return nil, false, err
		}

		b = buf.Bytes()
	}

	return b, compress, nil
}

func Marshal(c *Command) ([]byte, error) {
	return proto.Marshal(c)
}

func Unmarshal(b []byte, c *Command) error {
	return proto.Unmarshal(b, c)
}

func MarshalMetadataSet(c *MetadataSet) ([]byte, error) {
	return proto.Marshal(c)
}

func UnMarshalMetadataSet(b []byte, c *MetadataSet) error {
	return proto.Unmarshal(b, c)
}

func MarshalMetadataDelete(c *MetadataDelete) ([]byte, error) {
	return proto.Marshal(c)
}

func UnMarshalMetadataDelete(b []byte, c *MetadataDelete) error {
	return proto.Unmarshal(b, c)
}

// Assumes m is the is the right type....caller must use c.Type
func UnmarshalSubCommand(c *Command, m proto.Message) error {
	b := c.SubCommand
	if c.Compressed {
		gz, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return err
		}

		ub, err := ioutil.ReadAll(gz)
		if err != nil {
			return err
		}

		if err := gz.Close(); err != nil {
			return err
		}
		b = ub
	}

	return proto.Unmarshal(b, m)
}
