package http

import (
	"encoding/json"
	"errors"

	"github.com/rqlite/rqlite/v6/command"
)

var (
	// ErrNoStatements is returned when a request is empty
	ErrNoStatements = errors.New("no statements")

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

	simple := []string{}               // Represents a set of unparameterized queries
	parameterized := [][]interface{}{} // Represents a set of parameterized queries

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
		return nil, ErrInvalidRequest
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

		stmts[i].Parameters = make([]*command.Parameter, len(parameterized[i])-1)

		for j := range parameterized[i][1:] {
			switch v := parameterized[i][j+1].(type) {
			case int:
			case int64:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_I{
						I: v,
					},
				}
			case float64:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_D{
						D: v,
					},
				}
			case bool:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_B{
						B: v,
					},
				}
			case []byte:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_Y{
						Y: v,
					},
				}
			case string:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_S{
						S: v,
					},
				}
			default:
				return nil, ErrUnsupportedType
			}
		}
	}
	return stmts, nil

}
