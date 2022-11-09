package db

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/rqlite/go-sqlite3"
)

func Test_TableCreationInMemoryLoad(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	if !db.InMemory() {
		t.Fatal("in-memory database marked as not in-memory")
	}

	r, err := db.ExecuteStringStmt("CREATE TABLE logs (entry TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for table create, expected %s, got %s", exp, got)
	}

	go func() {
		for {
			r, err = db.ExecuteStringStmt(`INSERT INTO logs(entry) VALUES("hello")`)
			if err != nil {
				return
			}
		}
	}()

	n := 1
	for {
		res, err := db.QueryStringStmt("SELECT COUNT(*) FROM logs")
		if err != nil {
			t.Fatalf("failed to query table: %s", err.Error())
		}
		if exp, got := 1, len(res); exp != got {
			t.Fatalf("wrong number of rows returned, exp %d, got %d", exp, got)
		}
		if res[0].Error != "" {
			t.Fatalf("query rows has an error after %d queries: %s", n, res[0].Error)
		}
		n++
	}
}

func Test_TableCreationInMemoryLoadRaw(t *testing.T) {
	rwDSN := "file:/mydb?mode=rw&vfs=memdb&_txlock=immediate&_fk=false"
	roDSN := "file:/mydb?mode=ro&vfs=memdb&_txlock=deferred&_fk=false"

	rwDB, err := sql.Open("sqlite3", rwDSN)
	if err != nil {
		t.Fatalf("failed to open rw database: %s", err)
	}
	roDB, err := sql.Open("sqlite3", roDSN)
	if err != nil {
		t.Fatalf("failed to open ro database: %s", err)
	}

	rwConn, err := rwDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to create rw connection: %s", err)
	}
	defer rwConn.Close()
	roConn, err := roDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to create ro connection: %s", err)
	}
	defer roConn.Close()

	_, err = rwConn.ExecContext(context.Background(), `CREATE TABLE logs (entry TEXT)`, nil)
	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}

	go func() {
		for {
			var err error
			_, err = rwConn.ExecContext(context.Background(), `INSERT INTO logs(entry) VALUES("hello")`, nil)
			if err != nil {
				return
			}
		}
	}()

	for {
		rows, err := roConn.QueryContext(context.Background(), `SELECT COUNT(*) FROM logs`, nil)
		if err != nil {
			t.Fatalf("failed to query for count: %s", err)
		}

		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows had error after Next(): %s", err)

		}
		rows.Close()
	}
}
