package http

import (
	"encoding/json"
	"errors"

	"github.com/rqlite/rqlite/command"
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

// ParseRequest generates a set of Statements for a given byte slice.
func ParseRequest(b []byte) ([]*command.Statement, error) {
	if b == nil {
		return nil, ErrNoStatements
	}

	var simple []string               // Represents a set of unparameterized queries
	var parameterized [][]interface{} // Represents a set of parameterized queries

	// Try simple form first.
	err := json.Unmarshal(b, &simple)
	if err == nil {
		if len(simple) == 0 {
			return nil, ErrNoStatements
		}

		stmts := make([]*command.Statement, len(simple))
		for i := range simple {
			stmts[i] = &command.Statement{
				Sql: simple[i],
			}
		}
		return stmts, nil
	}

	// Next try parameterized form.
	if err := json.Unmarshal(b, &parameterized); err != nil {
		return nil, ErrInvalidJSON
	}
	stmts := make([]*command.Statement, len(parameterized))

	for i := range parameterized {
		if len(parameterized[i]) == 0 {
			return nil, ErrNoStatements
		}

		sql, ok := parameterized[i][0].(string)
		if !ok {
			return nil, ErrInvalidRequest
		}
		stmts[i] = &command.Statement{
			Sql:        sql,
			Parameters: nil,
		}
		if len(parameterized[i]) == 1 {
			// No actual parameters after the SQL string
			continue
		}

		stmts[i].Parameters = make([]*command.Parameter, 0)
		for j := range parameterized[i][1:] {
			m, ok := parameterized[i][j+1].(map[string]interface{})
			if ok {
				for k, v := range m {
					p, err := makeParameter(k, v)
					if err != nil {
						return nil, err
					}
					stmts[i].Parameters = append(stmts[i].Parameters, p)
				}
			} else {
				p, err := makeParameter("", parameterized[i][j+1])
				if err != nil {
					return nil, err
				}
				stmts[i].Parameters = append(stmts[i].Parameters, p)
			}
		}
	}
	return stmts, nil
}

func makeParameter(name string, i interface{}) (*command.Parameter, error) {
	switch v := i.(type) {
	case int:
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
	}
	return nil, ErrUnsupportedType
}
