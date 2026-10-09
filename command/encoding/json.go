package encoding

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/rqlite/rqlite/v10/command/proto"
)

var (
	// ErrTypesColumnsLengthViolation is returned when a results
	// object doesn't have the same number of types and columns
	ErrTypesColumnsLengthViolation = errors.New("types and columns are different lengths")
)

// ErrorFormat controls which error fields are rendered for a failed statement.
type ErrorFormat int

const (
	// ErrorFormatBoth renders both the legacy "error" string and the
	// structured "error_v2" object. This is the default.
	ErrorFormatBoth ErrorFormat = iota

	// ErrorFormatV1 renders only the legacy "error" string.
	ErrorFormatV1

	// ErrorFormatV2 renders only the structured "error_v2" object.
	ErrorFormatV2
)

// ErrorFormatFromString returns the ErrorFormat named by s. "v1" selects the
// legacy form and "v2" the structured form, ignoring case. Any other value,
// including the empty string, selects both.
func ErrorFormatFromString(s string) ErrorFormat {
	switch strings.ToLower(s) {
	case "v1":
		return ErrorFormatV1
	case "v2":
		return ErrorFormatV2
	default:
		return ErrorFormatBoth
	}
}

// Error represents the structured form of a statement failure. Message is
// always set. SQLite is present only when the failure originated in SQLite.
type Error struct {
	Message string            `json:"message"`
	SQLite  *SQLiteErrorCodes `json:"sqlite,omitempty"`
}

// SQLiteErrorCodes represents the result codes SQLite reported for a failure.
type SQLiteErrorCodes struct {
	Code         int32 `json:"code,omitempty"`
	ExtendedCode int32 `json:"extended_code,omitempty"`
	SystemErrno  int32 `json:"system_errno,omitempty"`
}

// ErrorResult represents a failed statement which produced no other output.
type ErrorResult struct {
	Error   string `json:"error,omitempty"`
	ErrorV2 *Error `json:"error_v2,omitempty"`
}

// Result represents the outcome of an operation that changes rows.
type Result struct {
	LastInsertID int64   `json:"last_insert_id,omitempty"`
	RowsAffected int64   `json:"rows_affected,omitempty"`
	Error        string  `json:"error,omitempty"`
	ErrorV2      *Error  `json:"error_v2,omitempty"`
	Time         float64 `json:"time,omitempty"`
}

// Rows represents the outcome of an operation that returns query data.
type Rows struct {
	Columns []string `json:"columns,omitempty"`
	Types   []string `json:"types,omitempty"`
	Values  [][]any  `json:"values,omitempty"`
	Error   string   `json:"error,omitempty"`
	ErrorV2 *Error   `json:"error_v2,omitempty"`
	Time    float64  `json:"time,omitempty"`
}

// AssociativeRows represents the outcome of an operation that returns query data.
type AssociativeRows struct {
	Types   map[string]string `json:"types,omitempty"`
	Rows    []map[string]any  `json:"rows"`
	Error   string            `json:"error,omitempty"`
	ErrorV2 *Error            `json:"error_v2,omitempty"`
	Time    float64           `json:"time,omitempty"`
}

// ResultWithRows represents the outcome of an operation that changes rows, but also
// includes an nil rows object, so clients can distinguish between a query and execute
// result.
type ResultWithRows struct {
	Result
	Rows []map[string]any `json:"rows"`
}

// ByteSliceAsArray is a byte slice that marshals to a JSON array of integers.
type ByteSliceAsArray []byte

// MarshalJSON implements the json.Marshaler interface. It marshals the byte slice as
// an array of integers.
func (b ByteSliceAsArray) MarshalJSON() ([]byte, error) {
	a := make([]int, len(b))
	for i, v := range b {
		a[i] = int(v)
	}
	return json.Marshal(a)
}

// NewResultRowsFromExecuteQueryResponse returns an API object from an
// ExecuteQueryResponse.
func NewResultRowsFromExecuteQueryResponse(e *proto.ExecuteQueryResponse, bytesAsArray bool, ef ErrorFormat) (any, error) {
	if er := e.GetE(); er != nil {
		return NewResultFromExecuteResult(er, ef)
	} else if qr := e.GetQ(); qr != nil {
		return NewRowsFromQueryRows(qr, bytesAsArray, ef)
	} else if err := e.GetError(); err != "" {
		return &ErrorResult{Error: err}, nil
	}
	return nil, errors.New("no ExecuteResult, QueryRows, or Error")
}

// NewAssociativeResultRowsFromExecuteQueryResponse returns an associative API
// object from an ExecuteQueryResponse.
func NewAssociativeResultRowsFromExecuteQueryResponse(e *proto.ExecuteQueryResponse, bytesAsArray bool, ef ErrorFormat) (any, error) {
	if er := e.GetE(); er != nil {
		if er.Error != "" {
			// A failed statement carries only its error. Omit the rows
			// field so the output matches that of a top-level error.
			msg, errV2 := errorFields(er.Error, er.ErrorV2, ef)
			return &ErrorResult{
				Error:   msg,
				ErrorV2: errV2,
			}, nil
		}
		r, err := NewResultFromExecuteResult(er, ef)
		if err != nil {
			return nil, err
		}
		return &ResultWithRows{
			Result: *r,
		}, nil
	} else if qr := e.GetQ(); qr != nil {
		return NewAssociativeRowsFromQueryRows(qr, bytesAsArray, ef)
	} else if err := e.GetError(); err != "" {
		return &ErrorResult{Error: err}, nil
	}
	return nil, errors.New("no ExecuteResult, QueryRows, or Error")
}

// errorFields returns the legacy error message and the structured error to
// render for a failed statement, as selected by ef.
func errorFields(msg string, e *proto.Error, ef ErrorFormat) (string, *Error) {
	switch ef {
	case ErrorFormatV1:
		return msg, nil
	case ErrorFormatV2:
		return "", NewErrorFromProto(e)
	default:
		return msg, NewErrorFromProto(e)
	}
}

// NewErrorFromProto returns an API Error object from a proto Error, or nil
// if there is no error.
func NewErrorFromProto(e *proto.Error) *Error {
	if e == nil {
		return nil
	}
	apiErr := &Error{
		Message: e.Message,
	}
	if se := e.Sqlite; se != nil {
		apiErr.SQLite = &SQLiteErrorCodes{
			Code:         se.Code,
			ExtendedCode: se.ExtendedCode,
			SystemErrno:  se.SystemErrno,
		}
	}
	return apiErr
}

// NewResultFromExecuteResult returns an API Result object from an ExecuteResult.
func NewResultFromExecuteResult(e *proto.ExecuteResult, ef ErrorFormat) (*Result, error) {
	msg, errV2 := errorFields(e.Error, e.ErrorV2, ef)
	return &Result{
		LastInsertID: e.LastInsertId,
		RowsAffected: e.RowsAffected,
		Error:        msg,
		ErrorV2:      errV2,
		Time:         e.Time,
	}, nil
}

// NewRowsFromQueryRows returns an API Rows object from a QueryRows
func NewRowsFromQueryRows(q *proto.QueryRows, bytesAsArray bool, ef ErrorFormat) (*Rows, error) {
	if len(q.Columns) != len(q.Types) {
		return nil, ErrTypesColumnsLengthViolation
	}

	values := make([][]any, len(q.Values))
	if err := NewValuesFromQueryValues(values, q.Values, bytesAsArray); err != nil {
		return nil, err
	}
	msg, errV2 := errorFields(q.Error, q.ErrorV2, ef)
	return &Rows{
		Columns: q.Columns,
		Types:   q.Types,
		Values:  values,
		Error:   msg,
		ErrorV2: errV2,
		Time:    q.Time,
	}, nil
}

// NewAssociativeRowsFromQueryRows returns an associative API object from a QueryRows
func NewAssociativeRowsFromQueryRows(q *proto.QueryRows, bytesAsArray bool, ef ErrorFormat) (*AssociativeRows, error) {
	if len(q.Columns) != len(q.Types) {
		return nil, ErrTypesColumnsLengthViolation
	}

	values := make([][]any, len(q.Values))
	if err := NewValuesFromQueryValues(values, q.Values, bytesAsArray); err != nil {
		return nil, err
	}

	rows := make([]map[string]any, len(values))
	for i := range rows {
		m := make(map[string]any)
		for ii, c := range q.Columns {
			m[c] = values[i][ii]
		}
		rows[i] = m
	}

	types := make(map[string]string)
	for i := range q.Types {
		types[q.Columns[i]] = q.Types[i]
	}

	msg, errV2 := errorFields(q.Error, q.ErrorV2, ef)
	return &AssociativeRows{
		Types:   types,
		Rows:    rows,
		Error:   msg,
		ErrorV2: errV2,
		Time:    q.Time,
	}, nil
}

// NewValuesFromQueryValues sets Values from a QueryValue object.
func NewValuesFromQueryValues(dest [][]any, v []*proto.Values, bytesAsArray bool) error {
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

		rowValues := make([]any, len(params))
		for p := range params {
			switch w := params[p].GetValue().(type) {
			case *proto.Parameter_I:
				rowValues[p] = w.I
			case *proto.Parameter_D:
				rowValues[p] = w.D
			case *proto.Parameter_B:
				rowValues[p] = w.B
			case *proto.Parameter_Y:
				if bytesAsArray {
					rowValues[p] = ByteSliceAsArray(w.Y)
				} else {
					rowValues[p] = w.Y
				}
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

// Encoder is used to JSON marshal ExecuteResults, QueryRows and ExecuteQueryRequests.
type Encoder struct {
	Associative       bool
	BlobsAsByteArrays bool
	Errors            ErrorFormat
}

// JSONMarshal implements the marshal interface
func (e *Encoder) JSONMarshal(i any) ([]byte, error) {
	return e.marshal(i, noEscapeEncode)
}

// JSONMarshalIndent implements the marshal indent interface
func (e *Encoder) JSONMarshalIndent(i any, prefix, indent string) ([]byte, error) {
	f := func(i any) ([]byte, error) {
		b, err := noEscapeEncode(i)
		if err != nil {
			return nil, err
		}
		var out bytes.Buffer
		if err := json.Indent(&out, b, prefix, indent); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}
	return e.marshal(i, f)
}

func noEscapeEncode(i any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(i); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

type marshalFunc func(i any) ([]byte, error)

func (e *Encoder) marshal(i any, f marshalFunc) ([]byte, error) {
	assoc, bytesAsArray, ef := e.Associative, e.BlobsAsByteArrays, e.Errors
	switch v := i.(type) {
	case *proto.ExecuteResult:
		r, err := NewResultFromExecuteResult(v, ef)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*proto.ExecuteResult:
		var err error
		results := make([]*Result, len(v))
		for j := range v {
			results[j], err = NewResultFromExecuteResult(v[j], ef)
			if err != nil {
				return nil, err
			}
		}
		return f(results)
	case *proto.QueryRows:
		if assoc {
			r, err := NewAssociativeRowsFromQueryRows(v, bytesAsArray, ef)
			if err != nil {
				return nil, err
			}
			return f(r)
		} else {
			r, err := NewRowsFromQueryRows(v, bytesAsArray, ef)
			if err != nil {
				return nil, err
			}
			return f(r)
		}
	case *proto.ExecuteQueryResponse:
		r, err := NewResultRowsFromExecuteQueryResponse(v, bytesAsArray, ef)
		if err != nil {
			return nil, err
		}
		return f(r)
	case []*proto.QueryRows:
		var err error

		if assoc {
			rows := make([]*AssociativeRows, len(v))
			for j := range v {
				rows[j], err = NewAssociativeRowsFromQueryRows(v[j], bytesAsArray, ef)
				if err != nil {
					return nil, err
				}
			}
			return f(rows)
		} else {
			rows := make([]*Rows, len(v))
			for j := range v {
				rows[j], err = NewRowsFromQueryRows(v[j], bytesAsArray, ef)
				if err != nil {
					return nil, err
				}
			}
			return f(rows)
		}
	case []*proto.ExecuteQueryResponse:
		if assoc {
			res := make([]any, len(v))
			for j := range v {
				r, err := NewAssociativeResultRowsFromExecuteQueryResponse(v[j], bytesAsArray, ef)
				if err != nil {
					return nil, err
				}
				res[j] = r
			}
			return f(res)
		} else {
			res := make([]any, len(v))
			for j := range v {
				r, err := NewResultRowsFromExecuteQueryResponse(v[j], bytesAsArray, ef)
				if err != nil {
					return nil, err
				}
				res[j] = r
			}
			return f(res)
		}
	case []*proto.Values:
		values := make([][]any, len(v))
		if err := NewValuesFromQueryValues(values, v, bytesAsArray); err != nil {
			return nil, err
		}
		return f(values)
	default:
		return f(v)
	}
}
