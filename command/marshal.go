package command

import (
	"bytes"
	"compress/gzip"
	"expvar"
	"fmt"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
)

const (
	DefaultBatchThreshold = 5
	DefaultSizeThreshold  = 150
)

type Requester interface {
	proto.Message
	GetRequest() *Request
}

type RequestMarshaler struct {
	BatchThreshold   int
	SizeThreshold    int
	ForceCompression bool

	gz *gzip.Writer
}

const (
	numRequests             = "num_requests"
	numCompressedRequests   = "num_compressed_requests"
	numUncompressedRequests = "num_uncompressed_requests"
	numCompressedBytes      = "num_compressed_bytes"
	numPrecompressedBytes   = "num_precompressed_bytes"
	numUncompressedBytes    = "num_uncompressed_bytes"
	numCompressionMisses    = "num_compression_misses"
)

// stats captures stats for the Proto marshaler.
var stats *expvar.Map

func init() {
	stats = expvar.NewMap("proto")
	stats.Add(numRequests, 0)
	stats.Add(numCompressedRequests, 0)
	stats.Add(numUncompressedRequests, 0)
	stats.Add(numCompressedBytes, 0)
	stats.Add(numUncompressedBytes, 0)
	stats.Add(numCompressionMisses, 0)
	stats.Add(numPrecompressedBytes, 0)
}

func NewRequestMarshaler() *RequestMarshaler {
	w, err := gzip.NewWriterLevel(nil, gzip.BestCompression)
	if err != nil {
		panic(fmt.Sprintf("failed to create GZIP writer: %s", err.Error()))
	}

	return &RequestMarshaler{
		BatchThreshold: DefaultBatchThreshold,
		SizeThreshold:  DefaultSizeThreshold,
		gz:             w,
	}
}

func (m *RequestMarshaler) Marshal(r Requester) ([]byte, bool, error) {
	stats.Add(numRequests, 0)
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
	ubz := len(b)
	stats.Add(numPrecompressedBytes, int64(ubz))

	if compress {
		// Let's try compression.
		var buf bytes.Buffer
		m.gz.Reset(&buf)
		if _, err := m.gz.Write(b); err != nil {
			return nil, false, err
		}
		if err := m.gz.Close(); err != nil {
			return nil, false, err
		}

		// Is compression better?
		if ubz > len(buf.Bytes()) || m.ForceCompression {
			// Yes! Let's keep it.
			b = buf.Bytes()
			stats.Add(numCompressedRequests, 1)
			stats.Add(numCompressedBytes, int64(len(b)))
		} else {
			// No. :-( Dump it.
			compress = false
			stats.Add(numCompressionMisses, 1)
		}
	} else {
		stats.Add(numUncompressedRequests, 1)
		stats.Add(numUncompressedBytes, int64(len(b)))
	}

	return b, compress, nil
}

func (m *RequestMarshaler) Stats() map[string]interface{} {
	return map[string]interface{}{
		"compression_size":  m.SizeThreshold,
		"compression_batch": m.BatchThreshold,
		"force_compression": m.ForceCompression,
	}
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
