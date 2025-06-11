package db

import (
	"os"
	"testing"
)

// Test_OpenSwappable_Success tests that OpenSwappable correctly opens a database and returns
// a valid SwappableDB instance.
func Test_OpenSwappable_Success(t *testing.T) {
	path := mustTempPath()
	defer os.Remove(path)

	// Attempt to open a swappable database
	swappableDB, err := OpenSwappable(path, nil, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Verify that the returned SwappableDB is not nil
	if swappableDB == nil {
		t.Fatalf("expected non-nil SwappableDB")
	}

	// Confirm a file was created at the specified path
	if !fileExists(path) {
		t.Fatalf("database file not created at %s", path)
	}

	// Check the paths of the underlying database
	if swappableDB.Path() != path {
		t.Fatalf("expected swappable database path to be %s, got %s", path, swappableDB.Path())
	}
}

// Test_OpenSwappable_InvalidPath tests that OpenSwappable returns an error when provided
// with an invalid file path.
func Test_OpenSwappable_InvalidPath(t *testing.T) {
	invalidPath := "/invalid/path/to/database"

	// Attempt to open a swappable database with an invalid path
	swappableDB, err := OpenSwappable(invalidPath, nil, false, false)
	if err == nil {
		swappableDB.Close()
		t.Fatalf("expected an error when opening swappable database with invalid path, got nil")
	}

	// Check that no SwappableDB instance is returned
	if swappableDB != nil {
		t.Fatalf("expected nil SwappableDB instance, got non-nil")
	}
}

// Test_SwapSuccess tests that the Swap function successfully swaps the underlying database.
func Test_SwapSuccess(t *testing.T) {
	// Create a new database with content
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	defer srcDB.Close()
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	mustExecute(srcDB, `INSERT INTO foo(name) VALUES("test")`)

	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Perform the swap
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database pre-swap: %s", err)
	}
	if err := swappableDB.Swap(srcPath, false, false); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	// Confirm the SwappableDB contains the data from the source database
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM foo")
	if err != nil {
		t.Fatalf("failed to query swapped database: %s", err)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"test"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected results after swap, expected %s, got %s", exp, got)
	}
}

func Test_SwapSuccess_Driver(t *testing.T) {
	// Create a new database and confirm foreign key support is enabled
	srcPath := mustTempPath()
	defer os.Remove(srcPath)
	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}
	defer srcDB.Close()
	mustExecute(srcDB, "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)")
	rows := mustQuery(srcDB, "PRAGMA foreign_keys")
	if exp, got := `[{"columns":["foreign_keys"],"types":["integer"],"values":[[0]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected foreign key support to be disabled, got %s", got)
	}

	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, ForeignKeyDriver(), false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Perform the swap
	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database pre-swap: %s", err)
	}
	if err := swappableDB.Swap(srcPath, false, false); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	// Confirm the SwappableDB still has the right FK setting, checking that it's using the right driver.
	rows, err = swappableDB.QueryStringStmt("PRAGMA foreign_keys")
	if err != nil {
		t.Fatalf("failed to query swapped database: %s", err)
	}
	if exp, got := `[{"columns":["foreign_keys"],"types":["integer"],"values":[[1]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected foreign key support to be enabled, got %s", got)
	}
}

// Test_SwapInvalidSQLiteFile tests that the Swap function returns an error when provided
// with an invalid SQLite file.
func Test_SwapInvalidSQLiteFile(t *testing.T) {
	// Create a SwappableDB with an empty database
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	// Create an invalid SQLite file
	invalidSQLiteFilePath := mustTempPath()
	defer os.Remove(invalidSQLiteFilePath)
	file, err := os.Create(invalidSQLiteFilePath)
	if err != nil {
		t.Fatalf("failed to create invalid SQLite file: %s", err)
	}
	if _, err := file.WriteString("not a valid SQLite file"); err != nil {
		t.Fatalf("failed to write to invalid SQLite file: %s", err)
	}
	file.Close()

	// Attempt to swap with the invalid SQLite file
	err = swappableDB.Swap(invalidSQLiteFilePath, false, false)
	if err == nil {
		t.Fatalf("expected an error when swapping with an invalid SQLite file, got nil")
	}
}

// Test_SwapFailurePreservesOriginal tests that if a swap fails, the original database
// is preserved and functional.
func Test_SwapFailurePreservesOriginal(t *testing.T) {
	// Helper function to execute statements on SwappableDB
	mustExecuteSwappable := func(db *SwappableDB, stmt string) {
		r, err := db.ExecuteStringStmt(stmt)
		if err != nil {
			t.Fatalf("failed to execute statement: %s", err.Error())
		}
		if len(r) > 0 && r[0].GetError() != "" {
			t.Fatalf("failed to execute statement: %s", r[0].GetError())
		}
	}

	// Create a SwappableDB with some data
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	swappableDB, err := OpenSwappable(swappablePath, nil, false, false)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()
	
	// Add some data to the original database
	mustExecuteSwappable(swappableDB, "CREATE TABLE original (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecuteSwappable(swappableDB, `INSERT INTO original(name) VALUES("original_data")`)

	// Verify original data exists
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM original")
	if err != nil {
		t.Fatalf("failed to query original database: %s", err)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"original_data"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("unexpected original data, expected %s, got %s", exp, got)
	}

	// Create a file that passes SQLite validation but will fail to open
	corruptPath := mustTempPath()
	defer os.Remove(corruptPath)
	
	// Create a file with SQLite header but invalid content
	corruptFile, err := os.Create(corruptPath)
	if err != nil {
		t.Fatalf("failed to create corrupt file: %s", err)
	}
	
	// Write SQLite file signature
	sqliteHeader := []byte("SQLite format 3\000")
	if _, err := corruptFile.Write(sqliteHeader); err != nil {
		t.Fatalf("failed to write SQLite header: %s", err)
	}
	
	// Write some invalid data that will make SQLite fail to parse the database
	invalidData := make([]byte, 1000)
	for i := range invalidData {
		invalidData[i] = 0xFF // Invalid content
	}
	if _, err := corruptFile.Write(invalidData); err != nil {
		t.Fatalf("failed to write invalid data: %s", err)
	}
	corruptFile.Close()

	// Verify that this file passes the IsValidSQLiteFile check but will fail to open
	if !IsValidSQLiteFile(corruptPath) {
		t.Fatalf("corrupt file should pass SQLite validation")
	}

	// Attempt to swap with the corrupt file - this should fail but preserve original
	err = swappableDB.Swap(corruptPath, false, false)
	if err == nil {
		t.Fatalf("expected swap to fail with corrupt database")
	}

	t.Logf("Swap failed as expected with error: %s", err)

	// Verify that the original database is still intact and functional
	rows, err = swappableDB.QueryStringStmt("SELECT * FROM original")
	if err != nil {
		t.Fatalf("failed to query original database after failed swap: %s", err)
	}
	if exp, got := `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"original_data"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("original data lost after failed swap, expected %s, got %s", exp, got)
	}

	// Verify that we can still add data to the original database
	mustExecuteSwappable(swappableDB, `INSERT INTO original(name) VALUES("more_data")`)
	
	rows, err = swappableDB.QueryStringStmt("SELECT COUNT(*) as count FROM original")
	if err != nil {
		t.Fatalf("failed to count rows in original database: %s", err)
	}
	if exp, got := `[{"columns":["count"],"types":["integer"],"values":[[2]]}]`, asJSON(rows); exp != got {
		t.Fatalf("expected 2 rows in original table, got %s", got)
	}
}

// Test_SwapWithWALFiles tests that the swap handles WAL files correctly during rollback.
func Test_SwapWithWALFiles(t *testing.T) {
	// Helper function to execute statements on SwappableDB
	mustExecuteSwappable := func(db *SwappableDB, stmt string) {
		r, err := db.ExecuteStringStmt(stmt)
		if err != nil {
			t.Fatalf("failed to execute statement: %s", err.Error())
		}
		if len(r) > 0 && r[0].GetError() != "" {
			t.Fatalf("failed to execute statement: %s", r[0].GetError())
		}
	}

	// Create a SwappableDB with WAL mode enabled
	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)
	defer os.Remove(swappablePath + "-wal")
	defer os.Remove(swappablePath + "-shm")
	
	swappableDB, err := OpenSwappable(swappablePath, nil, false, true) // WAL enabled
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()
	
	// Add some data to create WAL files
	mustExecuteSwappable(swappableDB, "CREATE TABLE wal_test (id INTEGER PRIMARY KEY, data TEXT)")
	mustExecuteSwappable(swappableDB, `INSERT INTO wal_test(data) VALUES("wal_data")`)

	// Verify WAL file exists
	walPath := swappablePath + "-wal"
	if !fileExists(walPath) {
		t.Fatalf("WAL file should exist at %s", walPath)
	}

	// Create a corrupt file for swap failure
	corruptPath := mustTempPath()
	defer os.Remove(corruptPath)
	
	corruptFile, err := os.Create(corruptPath)
	if err != nil {
		t.Fatalf("failed to create corrupt file: %s", err)
	}
	
	// Write SQLite file signature but invalid content
	sqliteHeader := []byte("SQLite format 3\000")
	if _, err := corruptFile.Write(sqliteHeader); err != nil {
		t.Fatalf("failed to write SQLite header: %s", err)
	}
	
	invalidData := make([]byte, 500)
	for i := range invalidData {
		invalidData[i] = 0xFF
	}
	if _, err := corruptFile.Write(invalidData); err != nil {
		t.Fatalf("failed to write invalid data: %s", err)
	}
	corruptFile.Close()

	// Attempt to swap with the corrupt file - this should fail and restore WAL files
	err = swappableDB.Swap(corruptPath, false, true)
	if err == nil {
		t.Fatalf("expected swap to fail with corrupt database")
	}

	// Verify that the original database with WAL is still functional
	rows, err := swappableDB.QueryStringStmt("SELECT * FROM wal_test")
	if err != nil {
		t.Fatalf("failed to query original database after failed swap: %s", err)
	}
	if exp, got := `[{"columns":["id","data"],"types":["integer","text"],"values":[[1,"wal_data"]]}]`, asJSON(rows); exp != got {
		t.Fatalf("original data lost after failed swap, expected %s, got %s", exp, got)
	}

	// Verify WAL file is still there
	if !fileExists(walPath) {
		t.Fatalf("WAL file should still exist after failed swap at %s", walPath)
	}
}
