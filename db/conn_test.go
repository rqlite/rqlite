package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
)

func Test_Conn_QueryTimeout(t *testing.T) {
	for _, api := range []string{"query", "request"} {
		for _, transaction := range []bool{false, true} {
			for _, requestDeadline := range []bool{false, true} {
				name := fmt.Sprintf("%s/transaction=%t/requestDeadline=%t", api, transaction, requestDeadline)
				t.Run(name, func(t *testing.T) {
					db, err := Open(filepath.Join(t.TempDir(), "test.db"), false, true)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()

					acquire := db.AcquireReader
					if api == "request" {
						acquire = db.AcquireWriter
					}
					conn, err := acquire(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()

					requestTimeout, parentTimeout := 2*time.Second, 20*time.Millisecond
					if requestDeadline {
						requestTimeout, parentTimeout = parentTimeout, requestTimeout
					}
					ctx, cancel := context.WithTimeout(context.Background(), parentTimeout)
					defer cancel()
					req := &command.Request{
						Transaction: transaction,
						DbTimeout:   int64(requestTimeout),
						Statements: []*command.Statement{
							{Sql: "SELECT 1", ForceStall: true},
						},
					}

					start := time.Now()
					var rows []*command.QueryRows
					if api == "query" {
						rows, err = conn.Query(ctx, req, false)
					} else {
						var results []*command.ExecuteQueryResponse
						results, err = conn.Request(ctx, req, false)
						for _, result := range results {
							rows = append(rows, result.GetQ())
						}
					}
					// A timed-out transaction may already have been rolled back by
					// database/sql when the API attempts to commit or roll it back.
					if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, sql.ErrTxDone) {
						t.Fatalf("unexpected error: %v", err)
					}
					if len(rows) != 1 || rows[0].GetError() != ErrQueryTimeout.Error() {
						t.Fatalf("expected query timeout, got rows=%v, err=%v", rows, err)
					}
					if elapsed := time.Since(start); elapsed >= time.Second {
						t.Fatalf("query did not honor the earlier deadline: elapsed %s", elapsed)
					}
					if requestDeadline && ctx.Err() != nil {
						t.Fatal("query waited for the parent deadline instead of the request timeout")
					}
				})
			}
		}
	}
}

func Test_Conn_TimeoutStopsLaterWrites(t *testing.T) {
	for _, api := range []string{"execute", "request"} {
		t.Run(api, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "test.db"), false, true)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mustExecute(db, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
			conn, err := db.AcquireWriter(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Once the first statement consumes the request's time budget, the
			// second statement must not receive a fresh timeout and commit a write.
			run := conn.Execute
			if api == "request" {
				run = conn.Request
			}
			results, err := run(ctx, &command.Request{
				DbTimeout: int64(20 * time.Millisecond),
				Statements: []*command.Statement{
					{Sql: "SELECT 1", ForceQuery: true, ForceStall: true},
					{Sql: "INSERT INTO foo VALUES (1)"},
				},
			}, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 2 || results[1].GetE().GetError() != ErrExecuteTimeout.Error() {
				t.Fatalf("expected later write to time out, got %v", results)
			}
			rows := mustQuery(db, "SELECT COUNT(*) FROM foo")
			if got := rows[0].Values[0].Parameters[0].GetI(); got != 0 {
				t.Fatalf("write committed after request timeout: row count %d", got)
			}
		})
	}
}
