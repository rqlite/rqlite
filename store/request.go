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

type QueryRequest2 struct {
	command *proto.QueryCommand
}

func NewQueryRequest2() *QueryRequest2 {
	return &QueryRequest2{
		command: &proto.QueryCommand{},
	}
}

func (q *QueryRequest2) SetTimings(b bool) {
	q.command.Timings = b
}

func (q *QueryRequest2) SetTransaction(b bool) {
	q.command.Transaction = b
}

func (q *QueryRequest2) SetSQL(sqls []string) (bool, error) {
	c := shouldCompress(sqls)
	if c {
		b, err := doCompress(sqls)
		if err != nil {
			return false, err
		}
		q.command.CompressedSqls = b
	} else {
		q.command.Sqls = sqls
	}

	return c, nil
}

func (q *QueryRequest2) GetTimings() bool { return q.command.Timings }

func (q *QueryRequest2) GetTransaction() bool { return q.command.Transaction }

func (q *QueryRequest2) GetSQL() ([]string, error) {
	if q.command.CompressedSqls != nil {
		return doDecompress(q.command.CompressedSqls)
	}
	return q.command.Sqls, nil
}

func (q *QueryRequest2) Marshal() ([]byte, error) {
	return pb.Marshal(q.command)
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
	if err := json.NewDecoder(gz).Decode(sqls); err != nil {
		return nil, err
	}
	return sqls, nil
}
