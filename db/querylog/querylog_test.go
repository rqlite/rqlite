package querylog

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

// --- Construction and lifecycle ---

func Test_QueryLogger_New(t *testing.T) {
	ql := New(DefaultConfig())
	if ql == nil {
		t.Fatal("expected QueryLogger to be created, got nil")
	}
	if ql.pending == nil {
		t.Fatal("expected pending map to be initialized")
	}
}

// Test_QueryLogger_NilLogger verifies that a nil logger is a supported
// runtime state: TraceHook must not panic when no log destination is set.
// This tests the QueryLogger runtime contract, not Config.
func Test_QueryLogger_NilLogger(t *testing.T) {
	ql := newWithLogger(DefaultConfig(), nil)

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
	// No panic == pass.
}

func Test_QueryLogger_Close(t *testing.T) {
	ql := New(DefaultConfig())
	if err := ql.Close(); err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}
}

// --- Trace behaviour ---

func Test_QueryLogger_StmtThenProfile(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero, NoExpandedSQL: false}, logger)

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x100,
		StmtHandle:    0x200,
		StmtOrTrigger: "INSERT INTO t VALUES (?)",
		ExpandedSQL:   "INSERT INTO t VALUES ('alice')",
	})
	if buf.Len() != 0 {
		t.Fatalf("expected no output after STMT, got: %s", buf.String())
	}

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x100,
		StmtHandle:     0x200,
		RunTimeNanosec: 30_000_000,
	})

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t VALUES ('alice')") {
		t.Fatalf("expected log to contain expanded SQL, got: %s", output)
	}
	if !strings.Contains(output, "(30ms)") {
		t.Fatalf("expected log to contain [30ms], got: %s", output)
	}
}

func Test_QueryLogger_ProfileWithoutStmt(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := newWithLogger(DefaultConfig(), logger)

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x100,
		StmtHandle:     0x200,
		RunTimeNanosec: 1_000_000,
	})

	if !strings.Contains(buf.String(), "PROFILE event without preceding STMT") {
		t.Fatalf("expected warning log for orphan PROFILE, got: %s", buf.String())
	}
}

func Test_QueryLogger_FallbackToStmtOrTrigger(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero}, logger)

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
	if !strings.Contains(output, "[0s]") {
		t.Fatalf("expected [0s] for zero-duration, got: %s", output)
	}
}

func Test_QueryLogger_MultipleConnections(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0xA, StmtHandle: 0x1, StmtOrTrigger: "SELECT 'conn_a'", ExpandedSQL: "SELECT 'conn_a'"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0xB, StmtHandle: 0x1, StmtOrTrigger: "SELECT 'conn_b'", ExpandedSQL: "SELECT 'conn_b'"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0xB, StmtHandle: 0x1, RunTimeNanosec: 2_000_000})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0xA, StmtHandle: 0x1, RunTimeNanosec: 5_000_000})

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
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x99, StmtOrTrigger: "INSERT INTO t VALUES (1)", ExpandedSQL: "INSERT INTO t VALUES (1)"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x99, RunTimeNanosec: 1_000_000})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x99, StmtOrTrigger: "DELETE FROM t WHERE id = 1", ExpandedSQL: "DELETE FROM t WHERE id = 1"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x99, RunTimeNanosec: 2_000_000})

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %s", len(lines), buf.String())
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
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, StmtOrTrigger: "", ExpandedSQL: ""})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 1_000_000})

	if !strings.Contains(buf.String(), "PROFILE event without preceding STMT") {
		t.Fatalf("expected warning for orphan PROFILE after empty SQL, got: %s", buf.String())
	}
}

func Test_QueryLogger_IgnoresOtherEvents(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := newWithLogger(DefaultConfig(), logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceRow, ConnHandle: 0x1, StmtHandle: 0x2})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceClose, ConnHandle: 0x1})

	if buf.Len() != 0 {
		t.Fatalf("expected no output for ROW/CLOSE events, got: %s", buf.String())
	}
}

func Test_QueryLogger_ConcurrentAccess(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero}, logger)

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
				ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: conn, StmtHandle: stmt, StmtOrTrigger: sql, ExpandedSQL: sql})
				ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: conn, StmtHandle: stmt, RunTimeNanosec: int64(i) * 1_000_000})
			}
		}(g)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != numGoroutines*numOps {
		t.Fatalf("expected %d log lines, got %d", numGoroutines*numOps, len(lines))
	}

	ql.mu.Lock()
	remaining := len(ql.pending)
	ql.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty pending map, got %d entries", remaining)
	}
}

// --- MinDuration filtering ---

func Test_QueryLogger_MinDuration_BelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	d := 10 * time.Millisecond
	ql := newWithLogger(&Config{MinDuration: &d}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, ExpandedSQL: "SELECT 1"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 5_000_000})

	if buf.Len() != 0 {
		t.Fatalf("expected no output for query below threshold, got: %s", buf.String())
	}

	ql.mu.Lock()
	remaining := len(ql.pending)
	ql.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty pending map after filtered query, got %d entries", remaining)
	}
}

func Test_QueryLogger_MinDuration_AtThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	d := 10 * time.Millisecond
	ql := newWithLogger(&Config{MinDuration: &d}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, ExpandedSQL: "SELECT 'at_threshold'"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 10_000_000})

	if !strings.Contains(buf.String(), "SELECT 'at_threshold'") {
		t.Fatalf("expected query at threshold to be logged, got: %s", buf.String())
	}
}

func Test_QueryLogger_MinDuration_AboveThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	d := 10 * time.Millisecond
	ql := newWithLogger(&Config{MinDuration: &d}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, ExpandedSQL: "SELECT 'above_threshold'"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 50_000_000})

	output := buf.String()
	if !strings.Contains(output, "SELECT 'above_threshold'") {
		t.Fatalf("expected query above threshold to be logged, got: %s", output)
	}
	if !strings.Contains(output, "[50ms]") {
		t.Fatalf("expected [50ms] in output, got: %s", output)
	}
}

func Test_QueryLogger_MinDuration_Zero_LogsEverything(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, ExpandedSQL: "SELECT 'zero_threshold'"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 0})

	if !strings.Contains(buf.String(), "SELECT 'zero_threshold'") {
		t.Fatalf("expected zero-threshold query to be logged, got: %s", buf.String())
	}
}

func Test_QueryLogger_MinDuration_PendingCleanedOnFilter(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	d := time.Second
	ql := newWithLogger(&Config{MinDuration: &d}, logger)

	for i := uintptr(1); i <= 3; i++ {
		ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: i, ExpandedSQL: fmt.Sprintf("SELECT %d", i)})
		ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: i, RunTimeNanosec: 1_000_000})
	}

	if buf.Len() != 0 {
		t.Fatalf("expected no output for all-filtered queries, got: %s", buf.String())
	}

	ql.mu.Lock()
	remaining := len(ql.pending)
	ql.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty pending map, got %d leaked entries", remaining)
	}
}

// --- NoExpandedSQL selection ---

func Test_QueryLogger_NoExpandedSQL_False_UsesExpanded(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero, NoExpandedSQL: false}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, StmtOrTrigger: "INSERT INTO t VALUES (?)", ExpandedSQL: "INSERT INTO t VALUES ('expanded')"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 1_000_000})

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t VALUES ('expanded')") {
		t.Fatalf("expected expanded SQL in output, got: %s", output)
	}
	if strings.Contains(output, "INSERT INTO t VALUES (?)") {
		t.Fatalf("should not see unexpanded SQL when NoExpandedSQL=false, got: %s", output)
	}
}

func Test_QueryLogger_NoExpandedSQL_True_UsesOriginal(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero, NoExpandedSQL: true}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, StmtOrTrigger: "INSERT INTO t VALUES (?)", ExpandedSQL: "INSERT INTO t VALUES ('expanded')"})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 1_000_000})

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t VALUES (?)") {
		t.Fatalf("expected original SQL when NoExpandedSQL=true, got: %s", output)
	}
	if strings.Contains(output, "'expanded'") {
		t.Fatalf("should not see expanded SQL when NoExpandedSQL=true, got: %s", output)
	}
}

func Test_QueryLogger_NoExpandedSQL_False_FallbackWhenUnavailable(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	zero := time.Duration(0)
	ql := newWithLogger(&Config{MinDuration: &zero, NoExpandedSQL: false}, logger)

	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceStmt, ConnHandle: 0x1, StmtHandle: 0x2, StmtOrTrigger: "PRAGMA journal_mode", ExpandedSQL: ""})
	ql.TraceHook(sqlite3.TraceInfo{EventCode: sqlite3.TraceProfile, ConnHandle: 0x1, StmtHandle: 0x2, RunTimeNanosec: 1_000_000})

	if !strings.Contains(buf.String(), "PRAGMA journal_mode") {
		t.Fatalf("expected fallback to StmtOrTrigger, got: %s", buf.String())
	}
}
