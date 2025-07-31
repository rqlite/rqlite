package store

import (
	"strings"
	"testing"
	"time"
)

// Test_SingleNodeTransactionRollbackOnError tests that when explicit transactions
// (BEGIN/COMMIT) fail partway through, the transaction is properly rolled back.
func Test_SingleNodeTransactionRollbackOnError(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	defer s.Close(true)
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	if err := s.Bootstrap(NewServer(s.ID(), s.Addr(), true)); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err.Error())
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("Error waiting for leader: %s", err)
	}

	// Create a transaction sequence that will fail partway through
	// This simulates a restore operation that starts a transaction but fails
	statements := []string{
		"PRAGMA foreign_keys=OFF",
		"BEGIN TRANSACTION",
		"CREATE TABLE foo (id integer not null primary key, name text)",
		"INSERT INTO foo VALUES(1,'test')",
		"INSERT INTO nonexistent_table VALUES(1,'this should fail')", // This will fail
		"COMMIT",
	}

	// Execute the failing transaction
	er := executeRequestFromStrings(statements, false, false)
	results, _, err := s.Execute(er)
	if err != nil {
		t.Fatalf("Execute should not return an error, but got: %v", err)
	}

	// Check that there was an error in one of the statements
	hasError := false
	hasCommitError := false
	t.Logf("Total statements: %d, total results: %d", len(statements), len(results))
	for i, result := range results {
		if result.GetE() != nil && result.GetE().Error != "" {
			hasError = true
			t.Logf("Statement %d had error: %s", i, result.GetE().Error)
		} else if result.GetError() != "" {
			hasError = true
			if strings.Contains(result.GetError(), "cannot commit - no transaction is active") {
				hasCommitError = true
			}
			t.Logf("Statement %d had error: %s", i, result.GetError())
		}
	}
	if !hasError {
		t.Fatalf("Expected an error in one of the SQL statements, but none was found")
	}

	// Verify that the COMMIT failed because transaction was rolled back
	if !hasCommitError {
		t.Fatalf("Expected COMMIT to fail with 'cannot commit - no transaction is active', but it didn't")
	}

	// The critical test: check if the table 'foo' exists
	// If the transaction was properly rolled back, it should NOT exist
	checkTableSQL := "SELECT name FROM sqlite_master WHERE type='table' AND name='foo'"
	checkQr := queryRequestFromString(checkTableSQL, false, false)
	checkResults, _, checkErr := s.Query(checkQr)
	if checkErr != nil {
		t.Fatalf("Check table query failed: %v", checkErr)
	}

	if len(checkResults) == 0 {
		t.Fatalf("Expected query results")
	}

	if checkResults[0].Error != "" {
		t.Fatalf("Check table query should succeed, but got error: %s", checkResults[0].Error)
	}

	// This is the main assertion - the table should NOT exist if transaction was rolled back
	if len(checkResults[0].Values) > 0 {
		t.Fatalf("Table 'foo' exists after failed transaction - transaction was NOT rolled back properly! Values: %v", checkResults[0].Values)
	}

	// Additional verification: ensure normal operations still work
	createTableSQL := "CREATE TABLE test_after_failure (id INTEGER PRIMARY KEY, name TEXT)"
	createEr := executeRequestFromString(createTableSQL, false, false)
	createResults, _, err := s.Execute(createEr)
	if err != nil {
		t.Fatalf("Create table after failed transaction failed: %v", err)
	}

	if len(createResults) == 0 {
		t.Fatalf("Expected create table results")
	}

	if createResults[0].GetE() != nil && createResults[0].GetE().Error != "" {
		t.Fatalf("Create table after failed transaction should succeed, but got error: %s", createResults[0].GetE().Error)
	}
}