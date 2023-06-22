package db

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/rqlite/rqlite/command"
	"github.com/rqlite/rqlite/command/encoding"
)

func Test_RemoveFiles(t *testing.T) {
	d := t.TempDir()
	mustCreateClosedFile(fmt.Sprintf("%s/foo", d))
	mustCreateClosedFile(fmt.Sprintf("%s/foo-wal", d))

	if err := RemoveFiles(fmt.Sprintf("%s/foo", d)); err != nil {
		t.Fatalf("failed to remove files: %s", err.Error())
	}

	files, err := ioutil.ReadDir(d)
	if err != nil {
		t.Fatalf("failed to read directory: %s", err.Error())
	}
	if len(files) != 0 {
		t.Fatalf("expected directory to be empty, but wasn't")
	}
}

// Test_TableCreationInMemoryFK ensures foreign key constraints work
func Test_TableCreationInMemoryFK(t *testing.T) {
	createTableFoo := "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"
	createTableBar := "CREATE TABLE bar (fooid INTEGER NOT NULL PRIMARY KEY, FOREIGN KEY(fooid) REFERENCES foo(id))"
	insertIntoBar := "INSERT INTO bar(fooid) VALUES(1)"

	db := mustCreateInMemoryDatabase()
	defer db.Close()

	r, err := db.ExecuteStringStmt(createTableFoo)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(createTableBar)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = db.ExecuteStringStmt(insertIntoBar)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	// Now, do same testing with FK constraints enabled.
	dbFK := mustCreateInMemoryDatabaseFK()
	defer dbFK.Close()
	if !dbFK.FKEnabled() {
		t.Fatal("FK constraints not marked as enabled")
	}

	r, err = dbFK.ExecuteStringStmt(createTableFoo)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = dbFK.ExecuteStringStmt(createTableBar)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}

	r, err = dbFK.ExecuteStringStmt(insertIntoBar)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err.Error())
	}
	if exp, got := `[{"error":"FOREIGN KEY constraint failed"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query, expected %s, got %s", exp, got)
	}
}

func Test_ConcurrentQueriesInMemory(t *testing.T) {
	db := mustCreateInMemoryDatabase()
	defer db.Close()

	r, err := db.ExecuteStringStmt(`CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}
	if exp, got := `[{}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}

	for i := 0; i < 5000; i++ {
		r, err = db.ExecuteStringStmt(`INSERT INTO foo(name) VALUES("fiona")`)
		if err != nil {
			t.Fatalf("failed to insert record: %s", err.Error())
		}
		if exp, got := fmt.Sprintf(`[{"last_insert_id":%d,"rows_affected":1}]`, i+1), asJSON(r); exp != got {
			t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ro, err := db.QueryStringStmt(`SELECT COUNT(*) FROM foo`)
			if err != nil {
				t.Logf("failed to query table: %s", err.Error())
			}
			if exp, got := `[{"columns":["COUNT(*)"],"types":[""],"values":[[5000]]}]`, asJSON(ro); exp != got {
				t.Logf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
			}
		}()
	}
	wg.Wait()
}

func Test_SimpleTransaction(t *testing.T) {
	db, path := mustCreateOnDiskDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(3, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"last_insert_id":3,"rows_affected":1},{"last_insert_id":4,"rows_affected":1}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"fiona"],[3,"fiona"],[4,"fiona"]]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func Test_PartialFailTransaction(t *testing.T) {
	db, path := mustCreateOnDiskDatabase()
	defer db.Close()
	defer os.Remove(path)

	_, err := db.ExecuteStringStmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %s", err.Error())
	}

	req := &command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(2, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(1, "fiona")`,
			},
			{
				Sql: `INSERT INTO foo(id, name) VALUES(4, "fiona")`,
			},
		},
	}
	r, err := db.Execute(req, false)
	if err != nil {
		t.Fatalf("failed to insert records: %s", err.Error())
	}
	if exp, got := `[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1},{"error":"UNIQUE constraint failed: foo.id"}]`, asJSON(r); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
	ro, err := db.QueryStringStmt(`SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("failed to query table: %s", err.Error())
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"]}]`, asJSON(ro); exp != got {
		t.Fatalf("unexpected results for query\nexp: %s\ngot: %s", exp, got)
	}
}

func mustCreateOnDiskDatabase() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, false, false)
	if err != nil {
		panic("failed to open database in DELETE mode")
	}

	return db, f
}

func mustCreateOnDiskDatabaseWAL() (*DB, string) {
	var err error
	f := mustTempFile()
	db, err := Open(f, false, true)
	if err != nil {
		panic("failed to open database in WAL mode")
	}

	return db, f
}

func mustCreateInMemoryDatabase() *DB {
	db, err := OpenInMemory(false)
	if err != nil {
		panic("failed to open in-memory database")
	}
	return db
}

func mustCreateInMemoryDatabaseFK() *DB {
	db, err := OpenInMemory(true)
	if err != nil {
		panic("failed to open in-memory database with foreign key constraints")
	}
	return db
}

// mustExecute executes a statement, and panics on failure. Used for statements
// that should never fail, even taking into account test setup.
func mustExecute(db *DB, stmt string) {
	r, err := db.ExecuteStringStmt(stmt)
	if err != nil {
		panic(fmt.Sprintf("failed to execute statement: %s", err.Error()))
	}
	if r[0].Error != "" {
		panic(fmt.Sprintf("failed to execute statement: %s", r[0].Error))
	}
}

func asJSON(v interface{}) string {
	enc := encoding.Encoder{}
	b, err := enc.JSONMarshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to JSON marshal value: %s", err.Error()))
	}
	return string(b)
}

// mustTempFile returns a path to a temporary file in directory dir. It is up to the
// caller to remove the file once it is no longer needed.
func mustTempFile() string {
	tmpfile, err := ioutil.TempFile("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	tmpfile.Close()
	return tmpfile.Name()
}

func mustTempDir() string {
	tmpdir, err := ioutil.TempDir("", "rqlite-db-test")
	if err != nil {
		panic(err.Error())
	}
	return tmpdir
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// function which copies a src file to a dst file, panics if any error
func mustCopyFile(dst, src string) {
	srcFile, err := os.Open(src)
	if err != nil {
		panic(err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		panic(err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		panic(err)
	}
}

func mustRenameFile(oldpath, newpath string) {
	if err := os.Rename(oldpath, newpath); err != nil {
		panic(err)
	}
}

func mustCreateClosedFile(path string) {
	f, err := os.Create(path)
	if err != nil {
		panic("failed to create file")
	}
	if err := f.Close(); err != nil {
		panic("failed to close file")
	}
}
