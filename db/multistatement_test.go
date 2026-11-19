package db

import (
	"fmt"
	"strings"
	"testing"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// The tests in this file exercise the patterns described in the "Multi-statement
// patterns" page of the rqlite documentation, at https://rqlite.io/docs/api/multi-statement/.
// Each test uses the same schema and, as far as is practical, the same SQL as that
// page. If a test here needs to change, the documentation probably does too.

// Test_MultiStatement_SingleStatement tests replacing a read followed by a write
// with a single statement.
func Test_MultiStatement_SingleStatement(t *testing.T) {
	db := mustCreateMultiStatementDB(t)

	t.Run("InsertSelect", func(t *testing.T) {
		r := msExecute(t, db, true,
			msStmt(`INSERT INTO audit_log(user_id, action) SELECT id, ? FROM users WHERE name = ?`, "login", "alice"),
		)
		msMustSucceed(t, r, 1)
		if exp, got := `[[1,"login"]]`, msValues(t, db, `SELECT user_id, action FROM audit_log`); exp != got {
			t.Fatalf("unexpected audit log\nexp: %s\ngot: %s", exp, got)
		}
	})

	t.Run("Upsert", func(t *testing.T) {
		upsert := msStmt(`INSERT INTO counters(name, n) VALUES(?, 1) ON CONFLICT(name) DO UPDATE SET n = n + 1`, "logins")
		for i := 1; i <= 3; i++ {
			msMustSucceed(t, msExecute(t, db, true, upsert), 1)
			if exp, got := fmt.Sprintf(`[["logins",%d]]`, i), msValues(t, db, `SELECT name, n FROM counters`); exp != got {
				t.Fatalf("unexpected counters after upsert %d\nexp: %s\ngot: %s", i, exp, got)
			}
		}
	})
}

// Test_MultiStatement_Returning tests that RETURNING hands generated values
// back to the caller.
func Test_MultiStatement_Returning(t *testing.T) {
	db := mustCreateMultiStatementDB(t)

	t.Run("Insert", func(t *testing.T) {
		r := msExecute(t, db, true,
			msReturning(`INSERT INTO users(name) VALUES(?) RETURNING id`, "bob"),
		)
		if exp, got := `[{"columns":["id"],"types":["integer"],"values":[[2]]}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results\nexp: %s\ngot: %s", exp, got)
		}
	})

	t.Run("Upsert", func(t *testing.T) {
		upsert := msReturning(`INSERT INTO counters(name, n) VALUES(?, 1) ON CONFLICT(name) DO UPDATE SET n = n + 1 RETURNING n`, "logins")
		for i := 1; i <= 2; i++ {
			r := msExecute(t, db, true, upsert)
			if exp, got := fmt.Sprintf(`[{"columns":["n"],"types":["integer"],"values":[[%d]]}]`, i), asJSON(r); exp != got {
				t.Fatalf("unexpected results for upsert %d\nexp: %s\ngot: %s", i, exp, got)
			}
		}
	})
}

// Test_MultiStatement_LastInsertRowID tests chaining inserts using
// last_insert_rowid(), and the conditions under which doing so is unsafe.
func Test_MultiStatement_LastInsertRowID(t *testing.T) {
	t.Run("Chained", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`INSERT INTO users(name) VALUES(?)`, "carol"),
			msStmt(`INSERT INTO profiles(user_id, bio) VALUES(last_insert_rowid(), ?)`, "hello"),
		)
		if exp, got := `[{"last_insert_id":2,"rows_affected":1},{"last_insert_id":1,"rows_affected":1}]`, asJSON(r); exp != got {
			t.Fatalf("unexpected results\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[2,"hello"]]`, msValues(t, db, `SELECT user_id, bio FROM profiles`); exp != got {
			t.Fatalf("unexpected profiles\nexp: %s\ngot: %s", exp, got)
		}
	})

	// With a transaction, a failed parent insert stops the request, so no
	// child row is written.
	t.Run("ParentFailsWithTransaction", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`INSERT INTO users(name) VALUES(?)`, "alice"),
			msStmt(`INSERT INTO profiles(user_id, bio) VALUES(last_insert_rowid(), ?)`, "orphan"),
		)
		msMustFailAt(t, r, 0, "UNIQUE constraint failed: users.name")
		if exp, got := `null`, msValues(t, db, `SELECT user_id, bio FROM profiles`); exp != got {
			t.Fatalf("unexpected profiles\nexp: %s\ngot: %s", exp, got)
		}
	})

	// Without a transaction, processing continues after the failed parent
	// insert, and the child row is attached to whichever row was inserted
	// most recently. This is why the pattern requires a transaction.
	t.Run("ParentFailsWithoutTransaction", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, false,
			msStmt(`INSERT INTO users(name) VALUES(?)`, "alice"),
			msStmt(`INSERT INTO profiles(user_id, bio) VALUES(last_insert_rowid(), ?)`, "orphan"),
		)
		if len(r) != 2 {
			t.Fatalf("expected 2 results, got %d: %s", len(r), asJSON(r))
		}
		if msError(r[0]) == "" {
			t.Fatalf("expected first statement to fail: %s", asJSON(r))
		}
		if msError(r[1]) != "" {
			t.Fatalf("expected second statement to succeed: %s", asJSON(r))
		}
		if got := msValues(t, db, `SELECT bio FROM profiles`); got != `[["orphan"]]` {
			t.Fatalf("expected orphan profile to be written, got: %s", got)
		}
	})

	// last_insert_rowid() only holds its value until the next insert, so a
	// third statement sees the row ID of the profile, not of the user.
	t.Run("ValueChangesAfterNextInsert", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`INSERT INTO users(name) VALUES(?)`, "carol"),
			msStmt(`INSERT INTO profiles(user_id, bio) VALUES(last_insert_rowid(), ?)`, "hello"),
			msStmt(`INSERT INTO audit_log(user_id, action) VALUES(last_insert_rowid(), ?)`, "signup"),
		)
		msMustSucceed(t, r, 3)
		if exp, got := `[[2]]`, msValues(t, db, `SELECT user_id FROM profiles`); exp != got {
			t.Fatalf("unexpected profile user ID\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1]]`, msValues(t, db, `SELECT user_id FROM audit_log`); exp != got {
			t.Fatalf("unexpected audit log user ID\nexp: %s\ngot: %s", exp, got)
		}
	})

	// An upsert which updates an existing row does not change the value.
	t.Run("ValueUnchangedByUpsertUpdate", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		upsert := msStmt(`INSERT INTO counters(name, n) VALUES(?, 1) ON CONFLICT(name) DO UPDATE SET n = n + 1`, "logins")
		msMustSucceed(t, msExecute(t, db, true, upsert), 1)
		r := msExecute(t, db, true,
			msStmt(`INSERT INTO users(name) VALUES(?)`, "carol"),
			upsert,
			msStmt(`INSERT INTO profiles(user_id, bio) VALUES(last_insert_rowid(), ?)`, "hello"),
		)
		msMustSucceed(t, r, 3)
		if exp, got := `[[2]]`, msValues(t, db, `SELECT user_id FROM profiles`); exp != got {
			t.Fatalf("unexpected profile user ID\nexp: %s\ngot: %s", exp, got)
		}
	})
}

// Test_MultiStatement_ApplicationKeys tests supplying a key generated by the
// application to every statement which needs it.
func Test_MultiStatement_ApplicationKeys(t *testing.T) {
	db := mustCreateMultiStatementDB(t)
	id := "0b9f5c1e-6d57-4b2a-9d43-7f1c2a8e4b10"
	r := msExecute(t, db, true,
		msStmt(`INSERT INTO orders(id, customer) VALUES(?, ?)`, id, "alice"),
		msStmt(`INSERT INTO order_items(order_id, sku, qty) VALUES(?, ?, ?)`, id, "A-100", 2),
	)
	msMustSucceed(t, r, 2)
	q := `SELECT customer, sku, qty FROM orders JOIN order_items ON orders.id = order_items.order_id`
	if exp, got := `[["alice","A-100",2]]`, msValues(t, db, q); exp != got {
		t.Fatalf("unexpected order\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_MultiStatement_TempTable tests using a temporary table to pass a value
// to several later statements.
func Test_MultiStatement_TempTable(t *testing.T) {
	t.Run("HoldsValue", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`INSERT INTO users(name) VALUES(?)`, "dave"),
			msStmt(`CREATE TEMP TABLE _vars AS SELECT last_insert_rowid() AS uid`),
			msStmt(`INSERT INTO profiles(user_id, bio) VALUES((SELECT uid FROM _vars), ?)`, "hi"),
			msStmt(`INSERT INTO audit_log(user_id, action) VALUES((SELECT uid FROM _vars), ?)`, "signup"),
			msStmt(`DROP TABLE _vars`),
		)
		msMustSucceed(t, r, 5)
		if exp, got := `[[2,"hi"]]`, msValues(t, db, `SELECT user_id, bio FROM profiles`); exp != got {
			t.Fatalf("unexpected profiles\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[2,"signup"]]`, msValues(t, db, `SELECT user_id, action FROM audit_log`); exp != got {
			t.Fatalf("unexpected audit log\nexp: %s\ngot: %s", exp, got)
		}
		msMustNotHaveTempTable(t, db, "_vars")
	})

	t.Run("HoldsSelectedRow", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`CREATE TEMP TABLE _vars AS SELECT id AS uid, name FROM users WHERE name = ?`, "alice"),
			msStmt(`INSERT INTO audit_log(user_id, action) VALUES((SELECT uid FROM _vars), ?)`, "login"),
			msStmt(`INSERT INTO profiles(user_id, bio) SELECT uid, name FROM _vars`),
			msStmt(`DROP TABLE _vars`),
		)
		msMustSucceed(t, r, 4)
		if exp, got := `[[1,"login"]]`, msValues(t, db, `SELECT user_id, action FROM audit_log`); exp != got {
			t.Fatalf("unexpected audit log\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1,"alice"]]`, msValues(t, db, `SELECT user_id, bio FROM profiles`); exp != got {
			t.Fatalf("unexpected profiles\nexp: %s\ngot: %s", exp, got)
		}
	})

	// If the request fails after the temporary table is created, the
	// rollback removes the table, so the same request can be sent again.
	t.Run("RemovedByRollback", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		stmts := []*command.Statement{
			msStmt(`CREATE TEMP TABLE _vars AS SELECT 1 AS uid`),
			msStmt(`INSERT INTO users(name) VALUES(?)`, "alice"),
			msStmt(`DROP TABLE _vars`),
		}
		for i := 0; i < 2; i++ {
			r := msExecute(t, db, true, stmts...)
			msMustFailAt(t, r, 1, "UNIQUE constraint failed: users.name")
			msMustNotHaveTempTable(t, db, "_vars")
		}
	})
}

// Test_MultiStatement_ConstraintAbort tests that a constraint violation rolls
// back every change made by the request.
func Test_MultiStatement_ConstraintAbort(t *testing.T) {
	db := mustCreateMultiStatementDB(t)
	r := msExecute(t, db, true,
		msStmt(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, 500, 2),
		msStmt(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, 500, 1),
	)
	msMustFailAt(t, r, 1, "CHECK constraint failed: balance >= 0")
	if exp, got := `[[1,100],[2,0]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
		t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
	}

	// A transfer the constraint allows is applied in full.
	r = msExecute(t, db, true,
		msStmt(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, 100, 2),
		msStmt(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, 100, 1),
	)
	msMustSucceed(t, r, 2)
	if exp, got := `[[1,0],[2,100]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
		t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_MultiStatement_IdempotencyKey tests that recording a request ID as the
// first statement of a request makes the request safe to send again.
func Test_MultiStatement_IdempotencyKey(t *testing.T) {
	db := mustCreateMultiStatementDB(t)
	transfer := func(requestID string) []*command.ExecuteQueryResponse {
		return msExecute(t, db, true,
			msStmt(`INSERT INTO requests(id) VALUES(?)`, requestID),
			msStmt(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, 10, 1),
			msStmt(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, 10, 2),
		)
	}

	msMustSucceed(t, transfer("c2a4e7d0-91b3-4f6e-8a5d-3e9b7c1f2a64"), 3)
	if exp, got := `[[1,90],[2,10]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
		t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
	}

	// Sending the same request again must change nothing.
	for i := 0; i < 2; i++ {
		msMustFailAt(t, transfer("c2a4e7d0-91b3-4f6e-8a5d-3e9b7c1f2a64"), 0, "UNIQUE constraint failed: requests.id")
		if exp, got := `[[1,90],[2,10]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
			t.Fatalf("unexpected balances after retry\nexp: %s\ngot: %s", exp, got)
		}
	}

	// A new request ID is a new request.
	msMustSucceed(t, transfer("e81f0b36-2d7a-4c19-a5f4-6b0d9e3c7a28"), 3)
	if exp, got := `[[1,80],[2,20]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
		t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_MultiStatement_Assertion tests using a temporary table with a CHECK
// constraint to abort a request unless a condition holds.
func Test_MultiStatement_Assertion(t *testing.T) {
	transfer := func(t *testing.T, db *DB, to int) []*command.ExecuteQueryResponse {
		return msExecute(t, db, true,
			msStmt(`CREATE TEMP TABLE _assert (ok INTEGER NOT NULL, CONSTRAINT account_exists CHECK(ok))`),
			msStmt(`INSERT INTO _assert SELECT EXISTS(SELECT 1 FROM accounts WHERE id = ?)`, to),
			msStmt(`DROP TABLE _assert`),
			msStmt(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, 10, 1),
			msStmt(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, 10, to),
		)
	}

	t.Run("ConditionFalse", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		for i := 0; i < 2; i++ {
			msMustFailAt(t, transfer(t, db, 3), 1, "CHECK constraint failed: account_exists")
			if exp, got := `[[1,100],[2,0]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
				t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
			}
			msMustNotHaveTempTable(t, db, "_assert")
		}
	})

	t.Run("ConditionTrue", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		msMustSucceed(t, transfer(t, db, 2), 5)
		if exp, got := `[[1,90],[2,10]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
			t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
		}
		msMustNotHaveTempTable(t, db, "_assert")
	})

	// Without the assertion the request succeeds, because an UPDATE which
	// matches no rows is not an error. The amount is removed from one
	// account and added to none.
	t.Run("WithoutAssertion", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, 10, 1),
			msStmt(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, 10, 3),
		)
		msMustSucceed(t, r, 2)
		if exp, got := `[[1,90],[2,0]]`, msValues(t, db, `SELECT id, balance FROM accounts`); exp != got {
			t.Fatalf("unexpected balances\nexp: %s\ngot: %s", exp, got)
		}
	})
}

// Test_MultiStatement_ClaimRow tests claiming an unowned row with a single
// UPDATE, using RETURNING to learn which row was claimed.
func Test_MultiStatement_ClaimRow(t *testing.T) {
	db := mustCreateMultiStatementDB(t)
	claim := func(owner string) string {
		return asJSON(msExecute(t, db, true,
			msReturning(`UPDATE jobs SET owner = ? WHERE id = (SELECT id FROM jobs WHERE owner IS NULL ORDER BY id LIMIT 1) RETURNING id, payload`, owner),
		))
	}

	if exp, got := `[{"columns":["id","payload"],"types":["integer","text"],"values":[[1,"resize image"]]}]`, claim("worker-1"); exp != got {
		t.Fatalf("unexpected results for first claim\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[{"columns":["id","payload"],"types":["integer","text"],"values":[[2,"send email"]]}]`, claim("worker-2"); exp != got {
		t.Fatalf("unexpected results for second claim\nexp: %s\ngot: %s", exp, got)
	}

	// Nothing is left to claim, so the statement succeeds but returns no rows.
	if exp, got := `[{"columns":["id","payload"],"types":["integer","text"]}]`, claim("worker-3"); exp != got {
		t.Fatalf("unexpected results for third claim\nexp: %s\ngot: %s", exp, got)
	}
	if exp, got := `[[1,"worker-1"],[2,"worker-2"]]`, msValues(t, db, `SELECT id, owner FROM jobs`); exp != got {
		t.Fatalf("unexpected job owners\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_MultiStatement_ReadWrite tests that a read in a transactional request
// sees the changes made by the statements before it.
func Test_MultiStatement_ReadWrite(t *testing.T) {
	db := mustCreateMultiStatementDB(t)
	r, err := db.Request(&command.Request{
		Transaction: true,
		Statements: []*command.Statement{
			msStmt(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, 30, 1),
			msStmt(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, 30, 2),
			msStmt(`SELECT id, balance FROM accounts`),
		},
	}, false)
	if err != nil {
		t.Fatalf("failed to execute request: %s", err.Error())
	}
	msMustSucceed(t, r, 3)
	for i := 0; i < 2; i++ {
		if exp, got := int64(1), r[i].GetE().GetRowsAffected(); exp != got {
			t.Fatalf("unexpected rows affected for statement %d, exp %d, got %d", i, exp, got)
		}
	}
	if exp, got := `{"columns":["id","balance"],"types":["integer","integer"],"values":[[1,70],[2,30]]}`, asJSON(r[2]); exp != got {
		t.Fatalf("unexpected results for read\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_MultiStatement_JSONEach tests passing a list of rows as a single
// JSON-encoded parameter.
func Test_MultiStatement_JSONEach(t *testing.T) {
	db := mustCreateMultiStatementDB(t)
	id := "7d3e1a52-4c8b-4e0f-b6a9-2f5d8c1e9a37"
	items := `[{"sku": "A-100", "qty": 2}, {"sku": "B-200", "qty": 1}, {"sku": "C-300", "qty": 5}]`

	r := msExecute(t, db, true,
		msStmt(`INSERT INTO orders(id, customer) VALUES(?, ?)`, id, "alice"),
		msStmt(`INSERT INTO order_items(order_id, sku, qty) SELECT ?, json_extract(value, ?), json_extract(value, ?) FROM json_each(?)`,
			id, "$.sku", "$.qty", items),
	)
	msMustSucceed(t, r, 2)
	if exp, got := int64(3), r[1].GetE().GetRowsAffected(); exp != got {
		t.Fatalf("unexpected rows affected, exp %d, got %d", exp, got)
	}
	if exp, got := `[["A-100",2],["B-200",1],["C-300",5]]`, msValues(t, db, `SELECT sku, qty FROM order_items ORDER BY sku`); exp != got {
		t.Fatalf("unexpected order items\nexp: %s\ngot: %s", exp, got)
	}

	// The same approach works for DELETE.
	r = msExecute(t, db, true,
		msStmt(`DELETE FROM order_items WHERE sku IN (SELECT value FROM json_each(?))`, `["A-100", "C-300"]`),
	)
	msMustSucceed(t, r, 1)
	if exp, got := `[["B-200",1]]`, msValues(t, db, `SELECT sku, qty FROM order_items`); exp != got {
		t.Fatalf("unexpected order items after delete\nexp: %s\ngot: %s", exp, got)
	}
}

// Test_MultiStatement_OptimisticConcurrency tests compare-and-set using a
// version column, for operations which cannot be performed in one request.
func Test_MultiStatement_OptimisticConcurrency(t *testing.T) {
	t.Run("SingleStatement", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		if exp, got := `[[100,0]]`, msValues(t, db, `SELECT balance, version FROM accounts WHERE id = 1`); exp != got {
			t.Fatalf("unexpected account\nexp: %s\ngot: %s", exp, got)
		}
		cas := func(balance, version int) int64 {
			r := msExecute(t, db, true,
				msStmt(`UPDATE accounts SET balance = ?, version = version + 1 WHERE id = ? AND version = ?`, balance, 1, version),
			)
			msMustSucceed(t, r, 1)
			return r[0].GetE().GetRowsAffected()
		}

		// The version is unchanged since the read, so the write is applied.
		if exp, got := int64(1), cas(90, 0); exp != got {
			t.Fatalf("unexpected rows affected, exp %d, got %d", exp, got)
		}
		// A second writer which read the same version must not be applied.
		if exp, got := int64(0), cas(80, 0); exp != got {
			t.Fatalf("unexpected rows affected for stale write, exp %d, got %d", exp, got)
		}
		if exp, got := `[[90,1]]`, msValues(t, db, `SELECT balance, version FROM accounts WHERE id = 1`); exp != got {
			t.Fatalf("unexpected account\nexp: %s\ngot: %s", exp, got)
		}
		// After reading again, the second writer succeeds.
		if exp, got := int64(1), cas(80, 1); exp != got {
			t.Fatalf("unexpected rows affected after retry, exp %d, got %d", exp, got)
		}
		if exp, got := `[[80,2]]`, msValues(t, db, `SELECT balance, version FROM accounts WHERE id = 1`); exp != got {
			t.Fatalf("unexpected account\nexp: %s\ngot: %s", exp, got)
		}
	})

	t.Run("MultipleStatements", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		write := func(balance, version int) []*command.ExecuteQueryResponse {
			return msExecute(t, db, true,
				msStmt(`CREATE TEMP TABLE _assert (ok INTEGER NOT NULL, CONSTRAINT version_unchanged CHECK(ok))`),
				msStmt(`INSERT INTO _assert SELECT EXISTS(SELECT 1 FROM accounts WHERE id = ? AND version = ?)`, 1, version),
				msStmt(`DROP TABLE _assert`),
				msStmt(`UPDATE accounts SET balance = ?, version = version + 1 WHERE id = ?`, balance, 1),
				msStmt(`INSERT INTO audit_log(user_id, action) VALUES(?, ?)`, 1, "balance adjusted"),
			)
		}

		msMustSucceed(t, write(90, 0), 5)
		// A stale version must roll back the whole request.
		msMustFailAt(t, write(80, 0), 1, "CHECK constraint failed: version_unchanged")
		if exp, got := `[[90,1]]`, msValues(t, db, `SELECT balance, version FROM accounts WHERE id = 1`); exp != got {
			t.Fatalf("unexpected account\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1]]`, msValues(t, db, `SELECT COUNT(*) FROM audit_log`); exp != got {
			t.Fatalf("unexpected audit log count\nexp: %s\ngot: %s", exp, got)
		}
		msMustSucceed(t, write(80, 1), 5)
		if exp, got := `[[80,2]]`, msValues(t, db, `SELECT balance, version FROM accounts WHERE id = 1`); exp != got {
			t.Fatalf("unexpected account\nexp: %s\ngot: %s", exp, got)
		}
	})

	// A version check in the WHERE clause of one statement does not protect
	// the other statements in the request, since updating no rows is not an
	// error. This is why a multi-statement write needs an assertion.
	t.Run("MultipleStatementsWithoutAssertion", func(t *testing.T) {
		db := mustCreateMultiStatementDB(t)
		r := msExecute(t, db, true,
			msStmt(`UPDATE accounts SET balance = ?, version = version + 1 WHERE id = ? AND version = ?`, 80, 1, 7),
			msStmt(`INSERT INTO audit_log(user_id, action) VALUES(?, ?)`, 1, "balance adjusted"),
		)
		msMustSucceed(t, r, 2)
		if exp, got := `[[100,0]]`, msValues(t, db, `SELECT balance, version FROM accounts WHERE id = 1`); exp != got {
			t.Fatalf("unexpected account\nexp: %s\ngot: %s", exp, got)
		}
		if exp, got := `[[1]]`, msValues(t, db, `SELECT COUNT(*) FROM audit_log`); exp != got {
			t.Fatalf("unexpected audit log count\nexp: %s\ngot: %s", exp, got)
		}
	})
}

// mustCreateMultiStatementDB returns a database containing the schema and
// data used by the examples in the documentation. The database is closed
// and removed when the test completes.
func mustCreateMultiStatementDB(t *testing.T) *DB {
	t.Helper()
	db, path := mustCreateOnDiskDatabaseWAL()
	t.Cleanup(func() {
		db.Close()
		fsutil.Remove(path)
	})

	r := msExecute(t, db, true,
		msStmt(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE)`),
		msStmt(`CREATE TABLE profiles (user_id INTEGER, bio TEXT)`),
		msStmt(`CREATE TABLE audit_log (user_id INTEGER, action TEXT)`),
		msStmt(`CREATE TABLE counters (name TEXT PRIMARY KEY, n INTEGER)`),
		msStmt(`CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER CHECK(balance >= 0), version INTEGER NOT NULL DEFAULT 0)`),
		msStmt(`CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT)`),
		msStmt(`CREATE TABLE order_items (order_id TEXT, sku TEXT, qty INTEGER)`),
		msStmt(`CREATE TABLE requests (id TEXT PRIMARY KEY)`),
		msStmt(`CREATE TABLE jobs (id INTEGER PRIMARY KEY, payload TEXT, owner TEXT)`),
		msStmt(`INSERT INTO users(name) VALUES(?)`, "alice"),
		msStmt(`INSERT INTO accounts(id, balance) VALUES(1, 100), (2, 0)`),
		msStmt(`INSERT INTO jobs(payload) VALUES(?), (?)`, "resize image", "send email"),
	)
	msMustSucceed(t, r, 12)
	return db
}

// msStmt returns a statement with the given positional parameters, each of
// which must be an int or a string.
func msStmt(sql string, params ...any) *command.Statement {
	stmt := &command.Statement{Sql: sql}
	for _, p := range params {
		switch v := p.(type) {
		case int:
			stmt.Parameters = append(stmt.Parameters, &command.Parameter{
				Value: &command.Parameter_I{I: int64(v)},
			})
		case string:
			stmt.Parameters = append(stmt.Parameters, &command.Parameter{
				Value: &command.Parameter_S{S: v},
			})
		default:
			panic(fmt.Sprintf("unsupported parameter type %T", p))
		}
	}
	return stmt
}

// msReturning returns a statement which has a RETURNING clause, and so must
// have its rows returned to the caller.
func msReturning(sql string, params ...any) *command.Statement {
	stmt := msStmt(sql, params...)
	stmt.ForceQuery = true
	return stmt
}

// msExecute executes the statements in a single request, optionally within
// a transaction, and returns the results.
func msExecute(t *testing.T, db *DB, tx bool, stmts ...*command.Statement) []*command.ExecuteQueryResponse {
	t.Helper()
	r, err := db.Execute(&command.Request{
		Transaction: tx,
		Statements:  stmts,
	}, false)
	if err != nil {
		t.Fatalf("failed to execute request: %s", err.Error())
	}
	return r
}

// msError returns the error, if any, in a result.
func msError(r *command.ExecuteQueryResponse) string {
	if e := r.GetE().GetError(); e != "" {
		return e
	}
	if e := r.GetQ().GetError(); e != "" {
		return e
	}
	return r.GetError()
}

// msMustSucceed fails the test unless there are n results, none of which
// contains an error.
func msMustSucceed(t *testing.T, r []*command.ExecuteQueryResponse, n int) {
	t.Helper()
	if len(r) != n {
		t.Fatalf("expected %d results, got %d: %s", n, len(r), asJSON(r))
	}
	for i := range r {
		if e := msError(r[i]); e != "" {
			t.Fatalf("statement %d failed: %s", i, e)
		}
	}
}

// msMustFailAt fails the test unless statement i failed with the given
// error, every statement before it succeeded, and no statement after it
// was processed.
func msMustFailAt(t *testing.T, r []*command.ExecuteQueryResponse, i int, exp string) {
	t.Helper()
	if len(r) != i+1 {
		t.Fatalf("expected %d results, got %d: %s", i+1, len(r), asJSON(r))
	}
	for j := 0; j < i; j++ {
		if e := msError(r[j]); e != "" {
			t.Fatalf("statement %d failed: %s", j, e)
		}
	}
	if got := msError(r[i]); got != exp {
		t.Fatalf("unexpected error for statement %d\nexp: %s\ngot: %s", i, exp, got)
	}
}

// msValues returns the values returned by a query, as JSON. It returns "null"
// if the query returns no rows.
func msValues(t *testing.T, db *DB, query string) string {
	t.Helper()
	r := mustQuery(db, query)
	if len(r) != 1 {
		t.Fatalf("expected 1 result for query, got %d", len(r))
	}
	if r[0].GetError() != "" {
		t.Fatalf("query failed: %s", r[0].GetError())
	}
	j := asJSON(r)
	const marker = `"values":`
	i := strings.Index(j, marker)
	if i < 0 {
		return "null"
	}
	return strings.TrimSuffix(j[i+len(marker):], "}]")
}

// msMustNotHaveTempTable fails the test if the named temporary table exists
// on the connection used for writes.
func msMustNotHaveTempTable(t *testing.T, db *DB, name string) {
	t.Helper()
	r := msExecute(t, db, false, msReturning(`SELECT COUNT(*) FROM sqlite_temp_master WHERE name = ?`, name))
	if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[0]]}]`, asJSON(r); exp != got {
		t.Fatalf("temporary table %s exists\nexp: %s\ngot: %s", name, exp, got)
	}
}
