package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	command "github.com/rqlite/rqlite/v8/command/proto"
)

var (
	// ErrNoStatements is returned when a request is empty
	ErrNoStatements = errors.New("no statements")

	// ErrInvalidJSON is returned when a body is not valid JSON
	ErrInvalidJSON = errors.New("invalid JSON body")

	// ErrInvalidRequest is returned when a request cannot be parsed.
	ErrInvalidRequest = errors.New("invalid request")

	// ErrUnsupportedType is returned when a request contains an unsupported type.
	ErrUnsupportedType = errors.New("unsupported type")
)

// ParseRequest generates a set of Statements by decoding the data read
// from the given io.Reader.
func ParseRequest(r io.Reader) ([]*command.Statement, error) {
	if r == nil {
		return nil, ErrNoStatements
	}

	dec := json.NewDecoder(r)
	dec.UseNumber()
	t, err := dec.Token()
	if err != nil {
		return nil, ErrInvalidJSON
	}
	if t != json.Delim('[') {
		return nil, ErrInvalidRequest
	}

	// OK, we have confirmed we've got an array of statements. Next we need
	// to determine if the statements are simple strings, or parameterized
	// statements.
	var stmts []*command.Statement
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, ErrInvalidJSON
		}

		s, ok := t.(string)
		if ok {
			// Simple string statement.
			stmts = append(stmts, &command.Statement{Sql: s})
		} else if t == json.Delim('[') {
			// It's parameterized. We need to parse the array of objects, the
			// first of which is the SQL string, and the rest are the parameters.
			var items []interface{}
			for dec.More() {
				var item interface{}
				if err := dec.Decode(&item); err != nil {
					return nil, ErrInvalidJSON
				}
				items = append(items, item)
			}
			// Consume the closing bracket.
			t, err := dec.Token()
			if err != nil {
				return nil, ErrInvalidJSON
			}
			if t != json.Delim(']') {
				return nil, ErrInvalidRequest
			}

			// The first item should be the SQL string.
			if len(items) == 0 {
				return nil, ErrInvalidRequest
			}

			sql, ok := items[0].(string)
			if !ok {
				return nil, ErrInvalidRequest
			}

			stmt := &command.Statement{Sql: sql}
			if len(items) == 1 {
				stmts = append(stmts, stmt)
				continue
			}

			// The rest of the items should be the parameters.
			for i := range items[1:] {
				m, ok := items[i+1].(map[string]interface{})
				if ok {
					for k, v := range m {
						p, err := makeParameter(k, v)
						if err != nil {
							return nil, err
						}
						stmt.Parameters = append(stmt.Parameters, p)
					}
				} else {
					p, err := makeParameter("", items[i+1])
					if err != nil {
						return nil, err
					}
					stmt.Parameters = append(stmt.Parameters, p)
				}
			}
			stmts = append(stmts, stmt)
		} else {
			return nil, ErrInvalidRequest
		}
	}

	// Check that the array of statements is closed.
	_, err = dec.Token()
	if err != nil {
		return nil, ErrInvalidJSON
	}

	if len(stmts) == 0 {
		return nil, ErrNoStatements
	}

	return stmts, nil
}

func makeParameter(name string, i interface{}) (*command.Parameter, error) {
	// Check if the value is a JSON number, and if so, convert it to an int64 or float64.
	// Then let the switch statement below handle it.
	if num, ok := i.(json.Number); ok {
		i64, err := num.Int64()
		if err == nil {
			i = i64
		} else {
			f64, err := num.Float64()
			if err != nil {
				return nil, fmt.Errorf("invalid number %s", num.String())
			}
			i = f64
		}
	}

	switch v := i.(type) {
	case int:
		return &command.Parameter{
			Value: &command.Parameter_I{
				I: int64(v),
			},
			Name: name,
		}, nil
	case int64:
		return &command.Parameter{
			Value: &command.Parameter_I{
				I: v,
			},
			Name: name,
		}, nil
	case float64:
		return &command.Parameter{
			Value: &command.Parameter_D{
				D: v,
			},
			Name: name,
		}, nil
	case bool:
		return &command.Parameter{
			Value: &command.Parameter_B{
				B: v,
			},
			Name: name,
		}, nil
	case []byte:
		return &command.Parameter{
			Value: &command.Parameter_Y{
				Y: v,
			},
			Name: name,
		}, nil
	case string:
		return &command.Parameter{
			Value: &command.Parameter_S{
				S: v,
			},
			Name: name,
		}, nil
	case nil:
		return &command.Parameter{
			Value: nil,
			Name:  name,
		}, nil
	}
	return nil, ErrUnsupportedType
}
