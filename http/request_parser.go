package http

import (
	"encoding/json"
	"errors"

	"github.com/rqlite/rqlite/store"
)

var (
	// ErrNoStatements is returned when a request is empty
	ErrNoStatements = errors.New("no statements")

	// ErrInvalidRequest is returned when a request cannot be parsed.
	ErrInvalidRequest = errors.New("invalid request")
)

// ParseRequest generates a set of Statements for a given byte slice.
func ParseRequest(b []byte) ([]store.Statement, error) {
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

		stmts := make([]store.Statement, len(simple))
		for i := range simple {
			stmts[i].SQL = simple[i]
		}
		return stmts, nil
	}

	// Next try parameterized form.
	if err := json.Unmarshal(b, &parameterized); err != nil {
		return nil, ErrInvalidRequest
	}
	stmts := make([]store.Statement, len(parameterized))

	for i := range parameterized {
		if len(parameterized[i]) == 0 {
			return nil, ErrNoStatements
		}

		stmts[i].SQL = parameterized[i][0].(string)
		if len(parameterized[i]) == 1 {
			continue
		}

		stmts[i].Parameters = make([]store.Value, len(parameterized[i])-1)

		for j := range parameterized[i][1:] {
			stmts[i].Parameters[j] = parameterized[i][j+1]
		}
	}
	return stmts, nil

}
