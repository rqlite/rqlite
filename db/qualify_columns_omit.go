//go:build !sqlite_column_metadata
// +build !sqlite_column_metadata

package db

import (
	"context"
	"database/sql"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

type qualifyColumnsKeyType struct{}

var qualifyColumnsKey = qualifyColumnsKeyType{}

// NewContextWithQualifyColumns returns a new context with the qualify columns flag set.
// Without the sqlite_column_metadata build tag, this is a no-op marker that will be
// ignored since qualifyRowColumns does nothing.
func NewContextWithQualifyColumns(ctx context.Context) context.Context {
	return context.WithValue(ctx, qualifyColumnsKey, true)
}

func shouldQualifyColumns(ctx context.Context) bool {
	v, _ := ctx.Value(qualifyColumnsKey).(bool)
	return v
}

// qualifyRowColumns is a no-op when built without the sqlite_column_metadata tag.
func (db *DB) qualifyRowColumns(_ *sql.Conn, _ string, _ *command.QueryRows) error {
	return nil
}
