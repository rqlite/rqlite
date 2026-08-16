package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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
	timer := time.NewTimer(e.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
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

func TestSlowQueryLogging(t *testing.T) {
	db, path := mustCreateOnDiskDatabase()
	defer db.Close()
	defer os.Remove(path)

	if got, want := db.slowQueryThreshold, defaultSlowQueryThreshold; got != want {
		t.Fatalf("unexpected default slow-query threshold: got %s, want %s", got, want)
	}

	var buf bytes.Buffer
	db.logger.SetOutput(&buf)
	db.logger.SetFlags(0)
	db.SetSlowQueryThreshold(5 * time.Millisecond)

	const executeStmt = "UPDATE foo SET name = 'bar'"
	_, err := db.executeStmtWithConn(
		context.Background(),
		&command.Statement{Sql: executeStmt},
		false,
		&slowQueryTestExecer{delay: 20 * time.Millisecond},
		0,
	)
	if err != nil {
		t.Fatalf("execute failed: %s", err.Error())
	}
	if got := buf.String(); !strings.Contains(got, "slow query:") || !strings.Contains(got, executeStmt) {
		t.Fatalf("execute did not log the full slow SQL statement: %q", got)
	}

	buf.Reset()
	const queryStmt = "SELECT 1"
	queryCtx, cancelQuery := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _ = db.QueryWithContext(queryCtx, &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        queryStmt,
				ForceStall: true,
			},
		},
	}, false)
	cancelQuery()
	if got := buf.String(); !strings.Contains(got, "slow query:") || !strings.Contains(got, queryStmt) {
		t.Fatalf("query did not log the full slow SQL statement: %q", got)
	}

	buf.Reset()
	forceQueryCtx, cancelForceQuery := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _ = db.ExecuteWithContext(forceQueryCtx, &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        queryStmt,
				ForceQuery: true,
				ForceStall: true,
			},
		},
	}, false)
	cancelForceQuery()
	if got := strings.Count(buf.String(), "slow query:"); got != 1 {
		t.Fatalf("force query logged %d times, want 1: %q", got, buf.String())
	}

	buf.Reset()
	db.SetSlowQueryThreshold(0)
	db.logSlowQuery(time.Now().Add(-time.Second), "SELECT 1")
	if got := buf.String(); got != "" {
		t.Fatalf("slow-query logging was not disabled: %q", got)
	}
}
