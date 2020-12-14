package store

import (
	"bytes"
	"compress/gzip"
	"encoding/json"

	pb "github.com/golang/protobuf/proto"
	"github.com/rqlite/rqlite/store/proto"
)

const (
	batchCompressSize = 5
	sqlCompressSize   = 100
)

type Request2 struct {
	command *proto.Command
}

func NewQueryRequest2() *Request2 {
	return &Request2{
		command: &proto.Command{
			Type: proto.Command_QUERY,
		},
	}
}

func NewExecuteRequest2() *Request2 {
	return &Request2{
		command: &proto.Command{
			Type: proto.Command_EXECUTE,
		},
	}
}

func NewRequest2() *Request2 {
	return &Request2{
		command: &proto.Command{},
	}
}

func (q *Request2) SetTimings(b bool) {
	q.command.Timings = b
}

func (q *Request2) SetTransaction(b bool) {
	q.command.Transaction = b
}

func (q *Request2) SetSQL(sqls []string) error {
	c := shouldCompress(sqls)
	if c {
		b, err := doCompress(sqls)
		if err != nil {
			return err
		}
		q.command.CompressedSqls = b
	} else {
		q.command.Sqls = sqls
	}

	return nil
}

func (q *Request2) GetTimings() bool { return q.command.Timings }

func (q *Request2) GetTransaction() bool { return q.command.Transaction }

func (q *Request2) GetSQL() ([]string, error) {
	if q.command.CompressedSqls != nil {
		return doDecompress(q.command.CompressedSqls)
	}
	return q.command.Sqls, nil
}

func (q *Request2) Compressed() bool {
	return q.command.CompressedSqls != nil
}

func (q *Request2) Marshal() ([]byte, error) {
	return pb.Marshal(q.command)
}

func UnmarshalRequest(b []byte) (*Request2, error) {
	c := &proto.Command{}
	err := pb.Unmarshal(b, c)
	if err != nil {
		return nil, err
	}

	return &Request2{
		command: c,
	}, nil
}

func shouldCompress(s []string) bool {
	if len(s) >= batchCompressSize {
		return true
	}

	for i := range s {
		if len(s[i]) >= sqlCompressSize {
			return true
		}
	}

	return false
}

func doCompress(sqls []string) ([]byte, error) {
	var b bytes.Buffer
	gz, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if err := json.NewEncoder(gz).Encode(sqls); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	// XXXX NEED TO COMPRESS PARAMETERS TOO!

	return b.Bytes(), nil
}

func doDecompress(b []byte) ([]string, error) {
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	sqls := make([]string, 0)
	if err := json.NewDecoder(gz).Decode(&sqls); err != nil {
		return nil, err
	}
	return sqls, nil
}
