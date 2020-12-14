package store

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"

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

func (r *Request2) SetTimings(b bool) {
	r.command.Timings = b
}

func (r *Request2) SetTransaction(b bool) {
	r.command.Transaction = b
}

func (r *Request2) SetSQL(sqls []string) error {
	c := shouldCompress(sqls)
	if c {
		b, err := doCompress(sqls)
		if err != nil {
			return err
		}
		r.command.CompressedSqls = b
	} else {
		r.command.Sqls = sqls
	}

	return nil
}

func (r *Request2) SetParameters(values [][]Value) error {
	if values == nil {
		return nil
	}

	r.command.Values = make([]*proto.Parameters, len(values))
	var p *proto.Parameters
	for i := range values {
		p = &proto.Parameters{}
		p.Values = make([]*proto.Parameter, len(values[i]))

		for ii := range values[i] {
			switch v := values[i][ii].(type) {
			case int:
			case int64:
				p.Values[ii] = &proto.Parameter{
					Value: &proto.Parameter_I{
						I: v,
					},
				}
			case float64:
				p.Values[ii] = &proto.Parameter{
					Value: &proto.Parameter_D{
						D: v,
					},
				}
			case bool:
				p.Values[ii] = &proto.Parameter{
					Value: &proto.Parameter_B{
						B: v,
					},
				}
			case []byte:
				p.Values[ii] = &proto.Parameter{
					Value: &proto.Parameter_Y{
						Y: v,
					},
				}
			case string:
				p.Values[ii] = &proto.Parameter{
					Value: &proto.Parameter_S{
						S: v,
					},
				}
			default:
				return fmt.Errorf("unsupported type: %T", v)
			}
		}

		r.command.Values[i] = p
	}

	return nil
}

func (r *Request2) GetTimings() bool { return r.command.Timings }

func (r *Request2) GetTransaction() bool { return r.command.Transaction }

func (r *Request2) GetSQL() ([]string, error) {
	if r.command.CompressedSqls != nil {
		return doDecompress(r.command.CompressedSqls)
	}
	return r.command.Sqls, nil
}

func (r *Request2) GetParameters() ([][]Value, error) {
	if r.command.Values == nil {
		return nil, nil
	}

	values := make([][]Value, len(r.command.Values))
	for i := range r.command.Values {
		v := make([]Value, len(r.command.Values[i].Values))
		for ii := range r.command.Values[i].Values {
			switch w := r.command.Values[i].Values[ii].GetValue().(type) {
			case *proto.Parameter_I:
				v[ii] = w
			case *proto.Parameter_D:
				v[ii] = w
			case *proto.Parameter_B:
				v[ii] = w
			case *proto.Parameter_Y:
				v[ii] = w
			case *proto.Parameter_S:
				v[ii] = w
			default:
				return nil, fmt.Errorf("unsupported type: %T", w)
			}
		}
		values[i] = v
	}
	return values, nil
}

func (r *Request2) Compressed() bool {
	return r.command.CompressedSqls != nil
}

func (r *Request2) Marshal() ([]byte, error) {
	return pb.Marshal(r.command)
}

func (r *Request2) JSON() ([]byte, error) {
	return json.Marshal(r.command)
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
