package db

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// These tests hold the guards that other checks stand behind: each one is
// reached directly, because the request-level tests are answered first by
// the check in front of it.

func Test_Changeset_LengthGuard(t *testing.T) {
	limit := math.MaxInt32 - 64
	if err := checkChangesetLen(limit); err != nil {
		t.Fatalf("%d bytes must fit: %v", limit, err)
	}
	if err := checkChangesetLen(limit + 1); !errors.Is(err, ErrChangesetTooLarge) {
		t.Fatalf("%d bytes must be refused, got %v", limit+1, err)
	}
}

// The refusal comes before anything reads the bytes, so the pages of this slice
// are never touched.
func Test_Changeset_ApplyRefusesOversize(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("needs a 64-bit int")
	}
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	err := a.ApplyChangeset(context.Background(), make([]byte, math.MaxInt32-63))
	if !errors.Is(err, ErrChangesetTooLarge) {
		t.Fatalf("expected ErrChangesetTooLarge, got %v", err)
	}
}

func cstring(t *testing.T, tls *libc.TLS, s string) uintptr {
	t.Helper()
	p, err := libc.CString(s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { libc.Xfree(tls, p) })
	return p
}

func Test_Changeset_AuthorizerDirect(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	ctx := context.Background()
	conn, err := a.AcquireWriter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw, err := conn.rawHandles()
	if err != nil {
		t.Fatal(err)
	}

	// A connection with no capture registered is never refused anything.
	if rc := authorizerCallback(raw.tls, 12345, sqlite3.SQLITE_INSERT, 0, 0, 0, 0); rc != sqlite3.SQLITE_OK {
		t.Fatalf("unregistered connection: rc %d", rc)
	}

	// Outside a transaction, everything is refused.
	auth := raw.installAuthorizer()
	if rc := authorizerCallback(raw.tls, raw.db, sqlite3.SQLITE_SELECT, 0, 0, 0, 0); rc != sqlite3.SQLITE_DENY || !auth.refused() {
		t.Fatalf("no transaction: rc %d, refused %v", rc, auth.refused())
	}
	raw.removeAuthorizer()

	if err := conn.exec(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.exec(ctx, "ROLLBACK")
	auth = raw.installAuthorizer()
	defer raw.removeAuthorizer()
	tbl := cstring(t, raw.tls, "foo")

	if rc := authorizerCallback(raw.tls, raw.db, sqlite3.SQLITE_INSERT, tbl, 0, cstring(t, raw.tls, "main"), 0); rc != sqlite3.SQLITE_OK {
		t.Fatalf("write to main: rc %d", rc)
	}
	if auth.refused() || len(auth.writtenTables()) != 1 || auth.writtenTables()[0] != "foo" {
		t.Fatalf("write to main: refused %v, tables %v", auth.refused(), auth.writtenTables())
	}
	if rc := authorizerCallback(raw.tls, raw.db, sqlite3.SQLITE_INSERT, tbl, 0, cstring(t, raw.tls, "temp"), 0); rc != sqlite3.SQLITE_DENY || !auth.refused() {
		t.Fatalf("write to temp: rc %d, refused %v", rc, auth.refused())
	}
}

func Test_Changeset_QueryAll(t *testing.T) {
	a, _ := newPair(t)
	conn, err := a.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	boom := errors.New("boom")
	var v int
	one := func() []any { return []any{&v} }
	const three = "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3"

	n := 0
	if err := queryAll(context.Background(), conn, three, nil, one, func() error { n++; return nil }); err != nil || n != 3 {
		t.Fatalf("plain run: %d rows, %v", n, err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := queryAll(cancelled, conn, three, nil, one, func() error { return nil }); err == nil {
		t.Fatal("a cancelled context must fail the query")
	}
	if err := queryAll(context.Background(), conn, three, nil, func() []any { return nil }, func() error { return nil }); err == nil {
		t.Fatal("a scan into the wrong number of destinations must fail")
	}
	if err := queryAll(context.Background(), conn, three, nil, one, func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("the row handler's error must come back, got %v", err)
	}

	// The context ends while rows remain: the iteration stops early, and only
	// the rows' own error says so.
	mid, stop := context.WithCancel(context.Background())
	defer stop()
	if err := queryAll(mid, conn, three, nil, one, func() error {
		stop()
		time.Sleep(200 * time.Millisecond)
		return nil
	}); err == nil {
		t.Fatal("an iteration cut short must report it")
	}
}

// failQueryAt makes the nth call of queryAll fail, and reports how many calls
// the function under test made when none failed.
func failQueryAt(n int, boom error) (calls *int, restore func()) {
	real, c := queryAll, 0
	queryAll = func(ctx context.Context, cn *Conn, q string, args []any, d func() []any, e func() error) error {
		c++
		if c == n {
			return boom
		}
		return real(ctx, cn, q, args, d, e)
	}
	return &c, func() { queryAll = real }
}

func Test_Changeset_SchemaQueriesFail(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	ctx := context.Background()
	conn, err := a.AcquireWriter(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	boom := errors.New("boom")

	for name, run := range map[string]func() error{
		"tableInfo": func() error { _, _, _, err := conn.tableInfo(ctx, "foo"); return err },
		"tablesRecordable": func() error {
			_, err := conn.tablesRecordable(ctx, []string{"foo"})
			return err
		},
		"checkChangesetShape": func() error {
			return conn.checkChangesetShape(ctx, []changesetTable{{name: "foo", pk: []bool{true, false}}})
		},
	} {
		calls, restore := failQueryAt(0, boom)
		if err := run(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		total := *calls
		restore()
		if total == 0 {
			t.Fatalf("%s made no query", name)
		}
		for n := 1; n <= total; n++ {
			_, restore := failQueryAt(n, boom)
			err := run()
			restore()
			if !errors.Is(err, boom) {
				t.Fatalf("%s: query %d of %d failed, got %v", name, n, total, err)
			}
		}
	}
}

func Test_Changeset_ReadChangeset(t *testing.T) {
	x, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	y, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	_, csX := mustCapture(t, x, csRequest(false, "INSERT INTO foo VALUES(1)"))
	_, csY := mustCapture(t, y, csRequest(false, "INSERT INTO foo VALUES(1, 'a')"))

	conn, err := x.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw, err := conn.rawHandles()
	if err != nil {
		t.Fatal(err)
	}

	tables, err := raw.readChangeset(csX)
	if err != nil || len(tables) != 1 || tables[0].name != "foo" || len(tables[0].pk) != 1 || !tables[0].pk[0] {
		t.Fatalf("valid changeset: %v, %v", tables, err)
	}

	// Cut inside a record.
	if _, err := raw.readChangeset(csX[:len(csX)-1]); !errors.Is(err, ErrChangesetMalformed) {
		t.Fatalf("truncated changeset: %v", err)
	}

	// One table named twice with two column counts.
	both := append(append([]byte(nil), csX...), csY...)
	if _, err := raw.readChangeset(both); !errors.Is(err, ErrChangesetMalformed) || !strings.Contains(err.Error(), "two column counts") {
		t.Fatalf("two column counts: %v", err)
	}

	real := cChangesetStart
	defer func() { cChangesetStart = real }()
	cChangesetStart = func(tls *libc.TLS, pp uintptr, n int32, p uintptr) int32 { return sqlite3.SQLITE_NOMEM }
	// A failure of this node is not a malformed changeset: it is fatal.
	var fatal *FatalError
	if _, err := raw.readChangeset(csX); errors.Is(err, ErrChangesetMalformed) || !errors.As(err, &fatal) {
		t.Fatalf("start failure: %v", err)
	}
}

func Test_Changeset_MissingTableIsNamed(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	empty, _ := mustCreateOnDiskDatabaseWAL()
	defer empty.Close()
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1)"))
	err := empty.ApplyChangeset(context.Background(), cs)
	if !errors.Is(err, ErrChangesetSchema) || !strings.Contains(err.Error(), "no table") {
		t.Fatalf("expected the missing table to be named, got %v", err)
	}
}
