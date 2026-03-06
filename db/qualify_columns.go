//go:build sqlite_column_metadata
// +build sqlite_column_metadata

package db

import (
	"context"
	"database/sql"

	sqlite3 "github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
)

type qualifyColumnsKeyType struct{}

var qualifyColumnsKey = qualifyColumnsKeyType{}

// NewContextWithQualifyColumns returns a new context with the qualify columns flag set.
func NewContextWithQualifyColumns(ctx context.Context) context.Context {
	return context.WithValue(ctx, qualifyColumnsKey, true)
}

func shouldQualifyColumns(ctx context.Context) bool {
	v, _ := ctx.Value(qualifyColumnsKey).(bool)
	return v
}

// qualifyRowColumns prefixes each column name in rows with its originating table name.
func (db *DB) qualifyRowColumns(conn *sql.Conn, query string, rows *command.QueryRows) error {
	if len(rows.Columns) == 0 {
		return nil
	}

	var tableNames []string
	err := conn.Raw(func(driverConn interface{}) error {
		sqliteConn := driverConn.(*sqlite3.SQLiteConn)
		stmt, err := sqliteConn.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()
		sqliteStmt := stmt.(*sqlite3.SQLiteStmt)
		tableNames = make([]string, len(rows.Columns))
		for i := range rows.Columns {
			tableNames[i] = sqliteStmt.ColumnTableName(i)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i, col := range rows.Columns {
		if tableNames[i] != "" {
			rows.Columns[i] = tableNames[i] + "." + col
		}
	}
	return nil
}
