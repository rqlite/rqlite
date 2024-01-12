package encoding

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rqlite/rqlite/v8/command/proto"
)

var (
	// ErrTypesColumnsLengthViolation is returned when a results
	// object doesn't have the same number of types and columns
	ErrTypesColumnsLengthViolation = errors.New("types and columns are different lengths")
)

// Result represents the outcome of an operation that changes rows.
type Result struct {
	LastInsertID int64   `json:"last_insert_id,omitempty"`
	RowsAffected int64   `json:"rows_affected,omitempty"`
	Error        string  `json:"error,omitempty"`
	Time         float64 `json:"time,omitempty"`
}

// Rows represents the outcome of an operation that returns query data.
type Rows struct {
	Columns []string        `json:"columns,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

// AssociativeRows represents the outcome of an operation that returns query data.
type AssociativeRows struct {
	Types map[string]string        `json:"types,omitempty"`
	Rows  []map[string]interface{} `json:"rows"`
	Error string                   `json:"error,omitempty"`
	Time  float64                  `json:"time,omitempty"`
}

// ResultWithRows represents the outcome of an operation that changes rows, but also
// includes an nil rows object, so clients can distinguish between a query and execute
// result.
type ResultWithRows struct {
	Result
	Rows []map[string]interface{} `json:"rows"`
}

// NewResultRowsFromExecuteQueryResponse returns an API object from an
// ExecuteQueryResponse.
func NewResultRowsFromExecuteQueryResponse(e *proto.ExecuteQueryResponse) (interface{}, error) {
	if er := e.GetE(); er != nil {
		return NewResultFromExecuteResult(er)
	} else if qr := e.GetQ(); qr != nil {
		return NewRowsFromQueryRows(qr)
	} else if err := e.GetError(); err != "" {
		return map[string]string{
			"error": err,
		}, nil
	}
	return nil, errors.New("no ExecuteResult, QueryRows, or Error")
}

func NewAssociativeResultRowsFromExecuteQueryResponse(e *proto.ExecuteQueryResponse) (interface{}, error) {
	if er := e.GetE(); er != nil {
		r, err := NewResultFromExecuteResult(er)
		if err != nil {
			return nil, err
		}
		return &ResultWithRows{
			Result: *r,
		}, nil
	} else if qr := e.GetQ(); qr != nil {
		return NewAssociativeRowsFromQueryRows(qr)
	} else if err := e.GetError(); err != "" {
		return map[string]string{
			"error": err,
		}, nil
	}
	return nil, errors.New("no ExecuteResult, QueryRows, or Error")
}

// NewResultFromExecuteResult returns an API Result object from an ExecuteResult.
func NewResultFromExecuteResult(e *proto.ExecuteResult) (*Result, error) {
	return &Result{
		LastInsertID: e.LastInsertId,
		RowsAffected: e.RowsAffected,
		Error:        e.Error,
		Time:         e.Time,
	}, nil
}

// NewRowsFromQueryRows returns an API Rows object from a QueryRows
func NewRowsFromQueryRows(q *proto.QueryRows) (*Rows, error) {
	if len(q.Columns) != len(q.Types) {
		return nil, ErrTypesColumnsLengthViolation
	}

	values := make([][]interface{}, len(q.Values))
	if err := NewValuesFromQueryValues(values, q.Values); err != nil {
		return nil, err
	}
	return &Rows{
		Columns: q.Columns,
		Types:   q.Types,
		Values:  values,
		Error:   q.Error,
		Time:    q.Time,
	}, nil
}

// NewAssociativeRowsFromQueryRows returns an associative API object from a QueryRows
func NewAssociativeRowsFromQueryRows(q *proto.QueryRows) (*AssociativeRows, error) {
	if len(q.Columns) != len(q.Types) {
		return nil, ErrTypesColumnsLengthViolation
	}

	values := make([][]interface{}, len(q.Values))
	if err := NewValuesFromQueryValues(values, q.Values); err != nil {
		return nil, err
	}

	rows := make([]map[string]interface{}, len(values))
	for i := range rows {
		m := make(map[string]interface{})
		for ii, c := range q.Columns {
			m[c] = values[i][ii]
		}
		rows[i] = m
	}

	types := make(map[string]string)
	for i := range q.Types {
		types[q.Columns[i]] = q.Types[i]
	}

	return &AssociativeRows{
		Types: types,
		Rows:  rows,
		Error: q.Error,
		Time:  q.Time,
	}, nil
}

// NewValuesFromQueryValues sets Values from a QueryValue object.
func NewValuesFromQueryValues(dest [][]interface{}, v []*proto.Values) error {
	for n := range v {
		vals := v[n]
		if vals == nil {
			dest[n] = nil
			continue
		}

		params := vals.GetParameters()
		if params == nil {
			dest[n] = nil
			continue
		}

		rowValues := make([]interface{}, len(params))
		for p := range params {
			switch w := params[p].GetValue().(type) {
			case *proto.Parameter_I:
				rowValues[p] = w.I
			case *proto.Parameter_D:
				rowValues[p] = w.D
			case *proto.Parameter_B:
				rowValues[p] = w.B
			case *proto.Parameter_Y:
				rowValues[p] = w.Y
			case *proto.Parameter_S:
				rowValues[p] = w.S
			case nil:
				rowValues[p] = nil
			default:
				return fmt.Errorf("unsupported parameter type at index %d: %T", p, w)
			}
		}
		dest[n] = rowValues
	}

	return nil
}

// Encoder is used to JSON marshal ExecuteResults, QueryRows
// and ExecuteQueryRequests.
type Encoder struct {
	Associative bool
}

// JSONMarshal implements the marshal interface
func (e *Encoder) JSONMarshal(i interface{}) ([]byte, error) {
	return jsonMarshal(i, noEscapeEncode, e.Associative)
}

// JSONMarshalIndent implements the marshal indent interface
func (e *Encoder) JSONMarshalIndent(i interface{}, prefix, indent string) ([]byte, error) {
	f := func(i interface{}) ([]byte, error) {
		b, err := noEscapeEncode(i)
		if err != nil {
			return nil, err
		}
		var out bytes.Buffer
		json.Indent(&out, b, prefix, indent)
		return out.Bytes(), nil
	}
	return jsonMarshal(i, f, e.Associative)
}

func noEscapeEncode(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(i); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

type marshalFunc func(i interface{}) ([]byte, error)

func jsonMarshal(i interface{}, f marshalFunc, assoc bool) ([]byte, error) {
	switch v := i.(type) {
	case *proto.ExecuteResult:
		r, err := NewResultFromExecuteResult(v)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*proto.ExecuteResult:
		var err error
		results := make([]*Result, len(v))
		for j := range v {
			results[j], err = NewResultFromExecuteResult(v[j])
			if err != nil {
				return nil, err
			}
		}
		return f(results)
	case *proto.QueryRows:
		if assoc {
			r, err := NewAssociativeRowsFromQueryRows(v)
			if err != nil {
				return nil, err
			}
			return f(r)
		} else {
			r, err := NewRowsFromQueryRows(v)
			if err != nil {
				return nil, err
			}
			return f(r)
		}
	case *proto.ExecuteQueryResponse:
		r, err := NewResultRowsFromExecuteQueryResponse(v)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*proto.QueryRows:
		var err error

		if assoc {
			rows := make([]*AssociativeRows, len(v))
			for j := range v {
				rows[j], err = NewAssociativeRowsFromQueryRows(v[j])
				if err != nil {
					return nil, err
				}
			}
			return f(rows)
		} else {
			rows := make([]*Rows, len(v))
			for j := range v {
				rows[j], err = NewRowsFromQueryRows(v[j])
				if err != nil {
					return nil, err
				}
			}
			return f(rows)
		}
	case []*proto.ExecuteQueryResponse:
		if assoc {
			res := make([]interface{}, len(v))
			for j := range v {
				r, err := NewAssociativeResultRowsFromExecuteQueryResponse(v[j])
				if err != nil {
					return nil, err
				}
				res[j] = r
			}
			return f(res)
		} else {
			res := make([]interface{}, len(v))
			for j := range v {
				r, err := NewResultRowsFromExecuteQueryResponse(v[j])
				if err != nil {
					return nil, err
				}
				res[j] = r
			}
			return f(res)
		}
	case []*proto.Values:
		values := make([][]interface{}, len(v))
		if err := NewValuesFromQueryValues(values, v); err != nil {
			return nil, err
		}
		return f(values)
	default:
		return f(v)
	}
}
