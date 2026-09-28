package db

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/db/querylog"
)

var driverSeq atomic.Int64

func testDriverName() string {
	return fmt.Sprintf("sqlite3-qlog-test-%d", driverSeq.Add(1))
}

// Test_QueryLog_Integration_Basic verifies that the driver correctly wires
// query logging without interfering with normal database operations.
func Test_QueryLog_Integration_Basic(t *testing.T) {
	ql := querylog.New(querylog.DefaultConfig())
	drv := NewDriverFromConfig(testDriverName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: ql,
	})
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	if _, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	if _, err = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('alice')"); err != nil {
		t.Fatalf("INSERT failed: %s", err)
	}
	rows, err := db.QueryStringStmt("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}
	if len(rows) != 1 || len(rows[0].Values) != 1 {
		t.Fatalf("expected 1 row, got %d result sets", len(rows))
	}
}

// Test_QueryLog_Integration_ZeroThreshold verifies that a zero MinDuration
// (log-everything) config does not panic or interfere with operations.
func Test_QueryLog_Integration_ZeroThreshold(t *testing.T) {
	zero := time.Duration(0)
	cfg := &querylog.Config{MinDuration: &zero}
	ql := querylog.New(cfg)
	drv := NewDriverFromConfig(testDriverName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: ql,
	})
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	if _, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	if _, err = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('bob')"); err != nil {
		t.Fatalf("INSERT failed: %s", err)
	}
	rows, err := db.QueryStringStmt("SELECT name FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}
	if len(rows) != 1 || len(rows[0].Values) != 1 {
		t.Fatalf("expected 1 row, got unexpected result")
	}
}

// Test_QueryLog_Integration_BulkRequest verifies query logging doesn't
// interfere with bulk statement requests.
func Test_QueryLog_Integration_BulkRequest(t *testing.T) {
	ql := querylog.New(querylog.DefaultConfig())
	drv := NewDriverFromConfig(testDriverName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: ql,
	})
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	if _, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	if _, err = db.RequestStringStmts([]string{
		"INSERT INTO t (val) VALUES (1)",
		"INSERT INTO t (val) VALUES (2)",
		"UPDATE t SET val = val + 10",
	}); err != nil {
		t.Fatalf("request failed: %s", err)
	}

	rows, err := db.QueryStringStmt("SELECT val FROM t ORDER BY val")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}
	if len(rows) != 1 || len(rows[0].Values) != 2 {
		t.Fatalf("expected 2 rows, got unexpected result")
	}
}

// Test_QueryLog_Integration_ConstraintViolation verifies that query logging
// does not interfere when a statement causes a constraint violation.
func Test_QueryLog_Integration_ConstraintViolation(t *testing.T) {
	ql := querylog.New(querylog.DefaultConfig())
	drv := NewDriverFromConfig(testDriverName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: ql,
	})
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	if _, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	if _, err = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('alice')"); err != nil {
		t.Fatalf("first INSERT failed: %s", err)
	}
	// Constraint violation — the duplicate insert should not panic the logger.
	res, _ := db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('alice')")
	if res == nil {
		t.Fatal("expected result set even on constraint violation")
	}
}

// Test_QueryLog_Integration_NilQueryLogger verifies that a nil QueryLogger
// in DriverConfig is handled gracefully (no trace hook installed).
func Test_QueryLog_Integration_NilQueryLogger(t *testing.T) {
	drv := NewDriverFromConfig(testDriverName(), &DriverConfig{
		ChkOnClose:  CnkOnCloseModeDisabled,
		QueryLogger: nil,
	})
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	if _, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
}
