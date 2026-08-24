package db

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func Test_QueryLogger_New(t *testing.T) {
	ql := NewQueryLogger(QueryLogConfig{})
	if ql == nil {
		t.Fatal("expected QueryLogger to be created, got nil")
	}
	if ql.pending == nil {
		t.Fatal("expected pending map to be initialized")
	}
}

func Test_QueryLogger_NilLogger(t *testing.T) {
	// With nil Logger, TraceHook should be a no-op — no panic.
	ql := NewQueryLogger(QueryLogConfig{Logger: nil})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x2,
		StmtOrTrigger: "SELECT 1",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 5_000_000,
	})
	// No panic = pass.
}

func Test_QueryLogger_StmtThenProfile(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// STMT event — provides SQL
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x100,
		StmtHandle:    0x200,
		StmtOrTrigger: "INSERT INTO t VALUES (?)",
		ExpandedSQL:   "INSERT INTO t VALUES ('alice')",
	})

	// Nothing logged yet (SQL is buffered, waiting for PROFILE)
	if buf.Len() != 0 {
		t.Fatalf("expected no output after STMT, got: %s", buf.String())
	}

	// PROFILE event — provides duration
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x100,
		StmtHandle:     0x200,
		RunTimeNanosec: 3_000_000, // 3ms
	})

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t VALUES ('alice')") {
		t.Fatalf("expected log to contain expanded SQL, got: %s", output)
	}
	if !strings.Contains(output, "[3ms]") {
		t.Fatalf("expected log to contain [3ms], got: %s", output)
	}
}

func Test_QueryLogger_ProfileWithoutStmt(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// PROFILE without preceding STMT — should produce no output
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x100,
		StmtHandle:     0x200,
		RunTimeNanosec: 1_000_000,
	})

	if buf.Len() != 0 {
		t.Fatalf("expected no output for orphan PROFILE, got: %s", buf.String())
	}
}

func Test_QueryLogger_FallbackToStmtOrTrigger(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// STMT with empty ExpandedSQL — should fall back to StmtOrTrigger
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x2,
		StmtOrTrigger: "PRAGMA journal_mode",
		ExpandedSQL:   "",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 0,
	})

	output := buf.String()
	if !strings.Contains(output, "PRAGMA journal_mode") {
		t.Fatalf("expected log to contain StmtOrTrigger text, got: %s", output)
	}
	if !strings.Contains(output, "[0ms]") {
		t.Fatalf("expected [0ms] for zero-duration, got: %s", output)
	}
}

func Test_QueryLogger_MultipleConnections(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// Two different connections executing simultaneously
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0xA,
		StmtHandle:    0x1,
		StmtOrTrigger: "SELECT 'conn_a'",
		ExpandedSQL:   "SELECT 'conn_a'",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0xB,
		StmtHandle:    0x1, // same StmtHandle, different ConnHandle
		StmtOrTrigger: "SELECT 'conn_b'",
		ExpandedSQL:   "SELECT 'conn_b'",
	})

	// PROFILE for conn B first
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0xB,
		StmtHandle:     0x1,
		RunTimeNanosec: 2_000_000,
	})
	// PROFILE for conn A
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0xA,
		StmtHandle:     0x1,
		RunTimeNanosec: 5_000_000,
	})

	output := buf.String()
	if !strings.Contains(output, "SELECT 'conn_b'") {
		t.Fatalf("expected conn_b SQL in log, got: %s", output)
	}
	if !strings.Contains(output, "SELECT 'conn_a'") {
		t.Fatalf("expected conn_a SQL in log, got: %s", output)
	}
}

func Test_QueryLogger_HandleReuse(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// First execution
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x99,
		StmtOrTrigger: "INSERT INTO t VALUES (1)",
		ExpandedSQL:   "INSERT INTO t VALUES (1)",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x99,
		RunTimeNanosec: 1_000_000,
	})

	// Second execution reusing same handle
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x99,
		StmtOrTrigger: "DELETE FROM t WHERE id = 1",
		ExpandedSQL:   "DELETE FROM t WHERE id = 1",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x99,
		RunTimeNanosec: 2_000_000,
	})

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %s", len(lines), output)
	}
	if !strings.Contains(lines[0], "INSERT INTO t VALUES (1)") {
		t.Fatalf("first line should be INSERT, got: %s", lines[0])
	}
	if !strings.Contains(lines[1], "DELETE FROM t WHERE id = 1") {
		t.Fatalf("second line should be DELETE, got: %s", lines[1])
	}
}

func Test_QueryLogger_EmptySQL(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// STMT with both SQL fields empty — should not buffer anything
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x2,
		StmtOrTrigger: "",
		ExpandedSQL:   "",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 1_000_000,
	})

	if buf.Len() != 0 {
		t.Fatalf("expected no output for empty SQL, got: %s", buf.String())
	}
}

func Test_QueryLogger_IgnoresOtherEvents(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// ROW and CLOSE events should be ignored
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:  sqlite3.TraceRow,
		ConnHandle: 0x1,
		StmtHandle: 0x2,
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:  sqlite3.TraceClose,
		ConnHandle: 0x1,
	})

	if buf.Len() != 0 {
		t.Fatalf("expected no output for ROW/CLOSE events, got: %s", buf.String())
	}
}

func Test_QueryLogger_ConcurrentAccess(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger})

	// Simulate multiple connections calling TraceHook concurrently.
	// This test is meaningful when run with -race.
	const numGoroutines = 10
	const numOps = 50

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				conn := uintptr(connID)
				stmt := uintptr(i)
				sql := fmt.Sprintf("SELECT %d FROM conn_%d", i, connID)

				ql.TraceHook(sqlite3.TraceInfo{
					EventCode:     sqlite3.TraceStmt,
					ConnHandle:    conn,
					StmtHandle:    stmt,
					StmtOrTrigger: sql,
					ExpandedSQL:   sql,
				})
				ql.TraceHook(sqlite3.TraceInfo{
					EventCode:      sqlite3.TraceProfile,
					ConnHandle:     conn,
					StmtHandle:     stmt,
					RunTimeNanosec: int64(i) * 1_000_000,
				})
			}
		}(g)
	}
	wg.Wait()

	// Verify we got the expected number of log lines
	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	expected := numGoroutines * numOps
	if len(lines) != expected {
		t.Fatalf("expected %d log lines, got %d", expected, len(lines))
	}

	// Verify pending map is empty (all STMT/PROFILE pairs resolved)
	ql.mu.Lock()
	remaining := len(ql.pending)
	ql.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty pending map, got %d entries", remaining)
	}
}
