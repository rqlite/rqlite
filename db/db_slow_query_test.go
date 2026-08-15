package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

type slowQueryTestExecer struct {
	delay time.Duration
}

func (e *slowQueryTestExecer) ExecContext(ctx context.Context, _ string, _ ...any) (sql.Result, error) {
	select {
	case <-time.After(e.delay):
		return slowQueryTestResult{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *slowQueryTestExecer) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

type slowQueryTestResult struct{}

func (slowQueryTestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (slowQueryTestResult) RowsAffected() (int64, error) {
	return 1, nil
}

func Test_SlowQueryLog_Execute(t *testing.T) {
	var buf bytes.Buffer
	db := &DB{
		logger: log.New(&buf, "", 0),
	}
	db.SetSlowQueryThreshold(10 * time.Millisecond)

	const query = "UPDATE foo SET name = 'bar'"

	_, err := db.executeStmtWithConn(
		context.Background(),
		&command.Statement{Sql: query},
		false,
		&slowQueryTestExecer{delay: 25 * time.Millisecond},
		0,
	)

	if err != nil {
		t.Fatalf("failed to execute statement: %s", err)
	}

	if !strings.Contains(buf.String(), "slow query") {
		t.Fatalf("expected slow query log, got %q", buf.String())
	}

	if !strings.Contains(buf.String(), query) {
		t.Fatalf("expected SQL statement in slow query log, got %q", buf.String())
	}
}

func Test_SlowQueryLog_Query(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer os.Remove(path)
	defer db.Close()

	var buf bytes.Buffer
	db.logger = log.New(&buf, "", 0)
	db.SetSlowQueryThreshold(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _ = db.QueryWithContext(ctx, &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        "SELECT 1",
				ForceStall: true,
			},
		},
	}, false)

	if count := strings.Count(buf.String(), "slow query"); count != 1 {
		t.Fatalf("expected one slow query log, got %d: %q", count, buf.String())
	}

	if !strings.Contains(buf.String(), "SELECT 1") {
		t.Fatalf("expected SQL statement in slow query log, got %q", buf.String())
	}
}

func Test_SwappableDB_SlowQueryThresholdPreservedOnSwap(t *testing.T) {
	srcPath := mustTempPath()
	defer os.Remove(srcPath)

	srcDB, err := Open(srcPath, false, false)
	if err != nil {
		t.Fatalf("failed to open source database: %s", err)
	}

	// Write to the database so the source file contains a valid SQLite header.
	if _, err := srcDB.ExecuteStringStmt("CREATE TABLE foo (id INTEGER)"); err != nil {
		t.Fatalf("failed to initialize source database: %s", err)
	}

	if err := srcDB.Close(); err != nil {
		t.Fatalf("failed to close source database: %s", err)
	}

	swappablePath := mustTempPath()
	defer os.Remove(swappablePath)

	swappableDB, err := OpenSwappable(swappablePath, nil, false, false, 0)
	if err != nil {
		t.Fatalf("failed to open swappable database: %s", err)
	}
	defer swappableDB.Close()

	threshold := 10 * time.Second
	swappableDB.SetSlowQueryThreshold(threshold)

	if got := swappableDB.db.slowQueryThreshold; got != threshold {
		t.Fatalf("expected slow query threshold %s, got %s", threshold, got)
	}

	if err := swappableDB.Swap(srcPath, false, false); err != nil {
		t.Fatalf("failed to swap database: %s", err)
	}

	if got := swappableDB.db.slowQueryThreshold; got != threshold {
		t.Fatalf("expected slow query threshold %s after swap, got %s", threshold, got)
	}
}

func Test_SlowQueryLog_Disabled(t *testing.T) {
	var buf bytes.Buffer
	db := &DB{
		logger: log.New(&buf, "", 0),
	}
	db.SetSlowQueryThreshold(0)

	db.logSlowQuery(time.Now().Add(-time.Minute), "SELECT 1")

	if buf.Len() != 0 {
		t.Fatalf("expected no slow query log when threshold is disabled, got %q", buf.String())
	}
}
