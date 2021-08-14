package encoding

import (
	"encoding/json"
	"fmt"

	"github.com/rqlite/rqlite/command"
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

// JSONMarshal serializes Execute and Query results to JSON API format.
func JSONMarshal(i interface{}) ([]byte, error) {
	return jsonMarshal(i, json.Marshal)
}

// JSONMarshalIndent serializes Execute and Query results to JSON API format,
// but also applies indent to the output.
func JSONMarshalIndent(i interface{}, prefix, indent string) ([]byte, error) {
	f := func(i interface{}) ([]byte, error) {
		return json.MarshalIndent(i, prefix, indent)
	}
	return jsonMarshal(i, f)
}

func jsonMarshal(i interface{}, f func(i interface{}) ([]byte, error)) ([]byte, error) {
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
		r, err := NewRowsFromQueryRows(v)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*command.QueryRows:
		var err error
		rows := make([]*Rows, len(v))
		for j := range v {
			rows[j], err = NewRowsFromQueryRows(v[j])
			if err != nil {
				return nil, err
			}
		}
		return f(rows)
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
