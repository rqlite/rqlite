package db

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"testing"
)

var driverSeq atomic.Int64

func testDriverName() string {
	return fmt.Sprintf("sqlite3-qlog-test-%d", driverSeq.Add(1))
}

func Test_QueryLog_Integration_Basic(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "[qlog] ", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	drv := newTestQueryLogDriver(testDriverName(), ql)
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	_, err = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %s", err)
	}
	_, err = db.QueryStringStmt("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}

	output := buf.String()
	if !strings.Contains(output, "CREATE TABLE t") {
		t.Fatalf("expected log to contain CREATE TABLE, got:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO t") {
		t.Fatalf("expected log to contain INSERT, got:\n%s", output)
	}
	if !strings.Contains(output, "SELECT id, name FROM t") {
		t.Fatalf("expected log to contain SELECT, got:\n%s", output)
	}
}

func Test_QueryLog_Integration_Disabled(t *testing.T) {
	ql := NewQueryLogger(QueryLogConfig{Logger: nil})
	drv := newTestQueryLogDriver(testDriverName(), ql)
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	_, err = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %s", err)
	}
	rows, err := db.QueryStringStmt("SELECT name FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %s", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(rows))
	}
	if len(rows[0].Values) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows[0].Values))
	}
}

func Test_QueryLog_Integration_BulkRequest(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})
	drv := newTestQueryLogDriver(testDriverName(), ql)
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}

	_, err = db.RequestStringStmts([]string{
		"INSERT INTO t (val) VALUES (1)",
		"INSERT INTO t (val) VALUES (2)",
		"UPDATE t SET val = val + 10",
	})
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t (val) VALUES (1)") {
		t.Fatalf("expected first INSERT in log, got:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO t (val) VALUES (2)") {
		t.Fatalf("expected second INSERT in log, got:\n%s", output)
	}
	if !strings.Contains(output, "UPDATE t SET val = val + 10") {
		t.Fatalf("expected UPDATE in log, got:\n%s", output)
	}
}

func Test_QueryLog_Integration_ConstraintViolation(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})
	drv := newTestQueryLogDriver(testDriverName(), ql)
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
	_, err = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('alice')")
	if err != nil {
		t.Fatalf("first INSERT failed: %s", err)
	}

	// Constraint violation — should still be logged
	_, _ = db.ExecuteStringStmt("INSERT INTO t (name) VALUES ('alice')")

	output := buf.String()
	count := strings.Count(output, "INSERT INTO t (name) VALUES ('alice')")
	if count != 2 {
		t.Fatalf("expected exactly 2 INSERT log entries, got %d in:\n%s", count, output)
	}
}

func Test_QueryLog_Integration_NilQueryLogger(t *testing.T) {
	drv := newTestQueryLogDriver(testDriverName(), nil)
	dbPath := t.TempDir() + "/test.db"

	db, err := OpenWithDriver(drv, dbPath, false, true)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	_, err = db.ExecuteStringStmt("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %s", err)
	}
}
