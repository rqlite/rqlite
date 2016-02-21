package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3" // required blank import
)

// DB is the SQL database.
type DB struct {
	conn *sql.DB
}

type Result sql.Result
type Row sql.Row
type Rows sql.Rows

// Open an existing database, creating it if it does not exist.
func Open(dbPath string) (*DB, error) {
	dbc, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	return &DB{
		conn: dbc,
	}, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Execute(queries []string, tx bool) ([]sql.Result, error) {
	type Execer interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
	}

	err := func() (err error) {
		var execer Execer
		defer func() {
			if t, ok := execer.(*sql.Tx); ok {
				if err != nil {
					t.Rollback()
					return
				}
				t.Commit()
			}
		}()

		if tx {
			execer, _ = db.conn.Begin()
		} else {
			execer = db.conn
		}

		for _, q := range queries {
			_, err = execer.Exec(q)
			if err != nil {
				return err
			}
		}
		return nil
	}()

	return nil, err
}

func (db *DB) Query(queries []string, tx bool) ([]*sql.Rows, error) {
	type Queryer interface {
		Query(query string, args ...interface{}) (*sql.Rows, error)
	}

	var allRows []*sql.Rows
	err := func() (err error) {
		var queryer Queryer
		defer func() {
			if t, ok := queryer.(*sql.Tx); ok {
				if err != nil {
					t.Rollback()
					return
				}
				t.Commit()
			}
		}()

		if tx {
			queryer, _ = db.conn.Begin()
		} else {
			queryer = db.conn
		}

		for _, q := range queries {
			rows, err := queryer.Query(q)
			if err != nil {
				return err
			}
			allRows = append(allRows, rows)
		}
		return nil
	}()

	return allRows, err
}
