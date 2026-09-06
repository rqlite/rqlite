package db

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

}

func Test_QueryLogger_StmtThenProfile(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, ExpandedSQL: true})

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

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x100,
		StmtHandle:     0x200,
		RunTimeNanosec: 1_000_000,
	})

	output := buf.String()
	if !strings.Contains(output, "PROFILE event without preceding STMT") {
		t.Fatalf("expected warning log for orphan PROFILE, got: %s", output)
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
	if !strings.Contains(output, "[0s]") {
		t.Fatalf("expected [0s] for zero-duration, got: %s", output)
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

	output := buf.String()
	// Since empty SQL isn't buffered, the PROFILE becomes orphaned and logs a warning
	if !strings.Contains(output, "PROFILE event without preceding STMT") {
		t.Fatalf("expected warning for orphan PROFILE after empty SQL, got: %s", output)
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

func Test_QueryLogger_MinDuration_BelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	// Threshold: 10ms — query runs for 5ms, should NOT be logged.
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, MinDuration: 10 * time.Millisecond, ExpandedSQL: true})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:   sqlite3.TraceStmt,
		ConnHandle:  0x1,
		StmtHandle:  0x2,
		ExpandedSQL: "SELECT 1",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 5_000_000, // 5ms
	})

	if buf.Len() != 0 {
		t.Fatalf("expected no output for query below threshold, got: %s", buf.String())
	}

	// Pending map must be empty — entry cleaned up even though query was filtered.
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
	// Threshold: 10ms — query runs for exactly 10ms, should be logged.
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, MinDuration: 10 * time.Millisecond, ExpandedSQL: true})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:   sqlite3.TraceStmt,
		ConnHandle:  0x1,
		StmtHandle:  0x2,
		ExpandedSQL: "SELECT 'at_threshold'",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 10_000_000, // exactly 10ms
	})

	output := buf.String()
	if !strings.Contains(output, "SELECT 'at_threshold'") {
		t.Fatalf("expected query at threshold to be logged, got: %s", output)
	}
}

func Test_QueryLogger_MinDuration_AboveThreshold(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	// Threshold: 10ms — query runs for 50ms, should be logged.
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, MinDuration: 10 * time.Millisecond, ExpandedSQL: true})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:   sqlite3.TraceStmt,
		ConnHandle:  0x1,
		StmtHandle:  0x2,
		ExpandedSQL: "SELECT 'above_threshold'",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 50_000_000, // 50ms
	})

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
	// MinDuration=0 means every query is logged regardless of duration.
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, MinDuration: 0, ExpandedSQL: true})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:   sqlite3.TraceStmt,
		ConnHandle:  0x1,
		StmtHandle:  0x2,
		ExpandedSQL: "SELECT 'zero_threshold'",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 0, // instantaneous
	})

	output := buf.String()
	if !strings.Contains(output, "SELECT 'zero_threshold'") {
		t.Fatalf("expected zero-threshold query to be logged, got: %s", output)
	}
}

func Test_QueryLogger_MinDuration_PendingCleanedOnFilter(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, MinDuration: 1 * time.Second, ExpandedSQL: true})

	// Inject three statements that will all be filtered (run < 1s).
	for i := uintptr(1); i <= 3; i++ {
		ql.TraceHook(sqlite3.TraceInfo{
			EventCode:   sqlite3.TraceStmt,
			ConnHandle:  0x1,
			StmtHandle:  i,
			ExpandedSQL: fmt.Sprintf("SELECT %d", i),
		})
		ql.TraceHook(sqlite3.TraceInfo{
			EventCode:      sqlite3.TraceProfile,
			ConnHandle:     0x1,
			StmtHandle:     i,
			RunTimeNanosec: 1_000_000, // 1ms — below 1s threshold
		})
	}

	// Nothing should be logged.
	if buf.Len() != 0 {
		t.Fatalf("expected no output for all-filtered queries, got: %s", buf.String())
	}

	// Pending map must be empty — no leaks from filtered entries.
	ql.mu.Lock()
	remaining := len(ql.pending)
	ql.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty pending map, got %d leaked entries", remaining)
	}
}

func Test_QueryLogger_ExpandedSQL_True_UsesExpanded(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, ExpandedSQL: true})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x2,
		StmtOrTrigger: "INSERT INTO t VALUES (?)",
		ExpandedSQL:   "INSERT INTO t VALUES ('expanded')",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 1_000_000,
	})

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t VALUES ('expanded')") {
		t.Fatalf("expected expanded SQL in output, got: %s", output)
	}
	if strings.Contains(output, "INSERT INTO t VALUES (?)") {
		t.Fatalf("should not see unexpanded SQL when ExpandedSQL=true, got: %s", output)
	}
}

func Test_QueryLogger_ExpandedSQL_False_UsesOriginal(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, ExpandedSQL: false})

	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x2,
		StmtOrTrigger: "INSERT INTO t VALUES (?)",
		ExpandedSQL:   "INSERT INTO t VALUES ('expanded')",
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 1_000_000,
	})

	output := buf.String()
	if !strings.Contains(output, "INSERT INTO t VALUES (?)") {
		t.Fatalf("expected original SQL when ExpandedSQL=false, got: %s", output)
	}
	if strings.Contains(output, "'expanded'") {
		t.Fatalf("should not see expanded SQL when ExpandedSQL=false, got: %s", output)
	}
}

func Test_QueryLogger_ExpandedSQL_True_FallbackWhenUnavailable(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)
	ql := NewQueryLogger(QueryLogConfig{Logger: logger, ExpandedSQL: true})

	// ExpandedSQL is empty — should fall back to StmtOrTrigger.
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:     sqlite3.TraceStmt,
		ConnHandle:    0x1,
		StmtHandle:    0x2,
		StmtOrTrigger: "PRAGMA journal_mode",
		ExpandedSQL:   "", // unavailable
	})
	ql.TraceHook(sqlite3.TraceInfo{
		EventCode:      sqlite3.TraceProfile,
		ConnHandle:     0x1,
		StmtHandle:     0x2,
		RunTimeNanosec: 1_000_000,
	})

	output := buf.String()
	if !strings.Contains(output, "PRAGMA journal_mode") {
		t.Fatalf("expected fallback to StmtOrTrigger, got: %s", output)
	}
}
