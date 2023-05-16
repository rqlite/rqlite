package encoding

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rqlite/rqlite/command"
)

var (
	// ErrTypesColumnsLengthViolation is returned when a results
	// object doesn't have the same number of types and columns
	ErrTypesColumnsLengthViolation = errors.New("types and columns are different lengths")
)

// ResultRows represents the outcome of an operation that might change rows or
// return query data.
type ResultRows struct {
	LastInsertID int64           `json:"last_insert_id,omitempty"`
	RowsAffected int64           `json:"rows_affected,omitempty"`
	Columns      []string        `json:"columns,omitempty"`
	Types        []string        `json:"types,omitempty"`
	Values       [][]interface{} `json:"values,omitempty"`
	Error        string          `json:"error,omitempty"`
	Time         float64         `json:"time,omitempty"`
}

type AssociativeResultRows struct {
	LastInsertID int64                    `json:"last_insert_id,omitempty"`
	RowsAffected int64                    `json:"rows_affected,omitempty"`
	Types        map[string]string        `json:"types,omitempty"`
	Rows         []map[string]interface{} `json:"rows,omitempty"`
	Error        string                   `json:"error,omitempty"`
	Time         float64                  `json:"time,omitempty"`
}

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
	Rows  []map[string]interface{} `json:"rows,omitempty"`
	Error string                   `json:"error,omitempty"`
	Time  float64                  `json:"time,omitempty"`
}

// NewResultRowsFromExecuteQueryResponse returns an API ResultRows object from an
// ExecuteQueryResponse.
func NewResultRowsFromExecuteQueryResponse(e *command.ExecuteQueryResponse) (*ResultRows, error) {
	er := e.GetE()
	qr := e.GetQ()

	if er != nil {
		return &ResultRows{
			LastInsertID: er.LastInsertId,
			RowsAffected: er.RowsAffected,
			Error:        er.Error,
			Time:         er.Time,
		}, nil
	} else if qr != nil {
		if len(qr.Columns) != len(qr.Types) {
			return nil, ErrTypesColumnsLengthViolation
		}
		values := make([][]interface{}, len(qr.Values))
		if err := NewValuesFromQueryValues(values, qr.Values); err != nil {
			return nil, err
		}
		return &ResultRows{
			Columns: qr.Columns,
			Types:   qr.Types,
			Values:  values,
			Error:   qr.Error,
			Time:    qr.Time,
		}, nil
	}
	return nil, errors.New("no ExecuteResult or QueryRows")
}

func NewAssociativeResultRowsFromExecuteQueryResponse(e *command.ExecuteQueryResponse) (*AssociativeResultRows, error) {
	er := e.GetE()
	qr := e.GetQ()

	if er != nil {
		return &AssociativeResultRows{
			LastInsertID: er.LastInsertId,
			RowsAffected: er.RowsAffected,
			Error:        er.Error,
			Time:         er.Time,
		}, nil
	} else if qr != nil {
		if len(qr.Columns) != len(qr.Types) {
			return nil, ErrTypesColumnsLengthViolation
		}
		values := make([][]interface{}, len(qr.Values))
		if err := NewValuesFromQueryValues(values, qr.Values); err != nil {
			return nil, err
		}

		rows := make([]map[string]interface{}, len(values))
		for i := range rows {
			m := make(map[string]interface{})
			for ii, c := range qr.Columns {
				m[c] = values[i][ii]
			}
			rows[i] = m
		}

		types := make(map[string]string)
		for i := range qr.Types {
			types[qr.Columns[i]] = qr.Types[i]
		}

		return &AssociativeResultRows{
			Types: types,
			Rows:  rows,
			Error: qr.Error,
			Time:  qr.Time,
		}, nil
	}

	return nil, errors.New("no ExecuteResult or QueryRows")
}

// NewResultFromExecuteResult returns an API Result object from an ExecuteResult.
func NewResultFromExecuteResult(e *command.ExecuteResult) (*Result, error) {
	return &Result{
		LastInsertID: e.LastInsertId,
		RowsAffected: e.RowsAffected,
		Error:        e.Error,
		Time:         e.Time,
	}, nil
}

// NewRowsFromQueryRows returns an API Rows object from a QueryRows
func NewRowsFromQueryRows(q *command.QueryRows) (*Rows, error) {
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
func NewAssociativeRowsFromQueryRows(q *command.QueryRows) (*AssociativeRows, error) {
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
func NewValuesFromQueryValues(dest [][]interface{}, v []*command.Values) error {
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
			case *command.Parameter_I:
				rowValues[p] = w.I
			case *command.Parameter_D:
				rowValues[p] = w.D
			case *command.Parameter_B:
				rowValues[p] = w.B
			case *command.Parameter_Y:
				rowValues[p] = w.Y
			case *command.Parameter_S:
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
	case *command.ExecuteResult:
		r, err := NewResultFromExecuteResult(v)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*command.ExecuteResult:
		var err error
		results := make([]*Result, len(v))
		for j := range v {
			results[j], err = NewResultFromExecuteResult(v[j])
			if err != nil {
				return nil, err
			}
		}
		return f(results)
	case *command.QueryRows:
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
	case *command.ExecuteQueryResponse:
		r, err := NewResultRowsFromExecuteQueryResponse(v)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*command.QueryRows:
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
	case []*command.ExecuteQueryResponse:
		if assoc {
			res := make([]*AssociativeResultRows, len(v))
			for j := range v {
				r, err := NewAssociativeResultRowsFromExecuteQueryResponse(v[j])
				if err != nil {
					return nil, err
				}
				res[j] = r
			}
			return f(res)
		} else {
			res := make([]*ResultRows, len(v))
			for j := range v {
				r, err := NewResultRowsFromExecuteQueryResponse(v[j])
				if err != nil {
					return nil, err
				}
				res[j] = r
			}
			return f(res)
		}
	case []*command.Values:
		values := make([][]interface{}, len(v))
		if err := NewValuesFromQueryValues(values, v); err != nil {
			return nil, err
		}
		return f(values)
	default:
		return f(v)
	}
}
