package db

import (
	"context"
	"errors"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

// These tests make one step of the changeset code fail at a time, which no real
// database can be made to do on demand, and check each failure is reported, leaves
// the data untouched, and leaves the connection usable.

func failMallocAt(n int) (fired *bool, restore func()) {
	real, calls, f := cMalloc, 0, false
	cMalloc = func(tls *libc.TLS, sz types.Size_t) uintptr {
		calls++
		if calls == n {
			f = true
			return 0
		}
		return real(tls, sz)
	}
	return &f, func() { cMalloc = real }
}

func Test_Changeset_MallocFails(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1, 'x')"))
	ctx := context.Background()

	for name, run := range map[string]func() error{
		"capture": func() error {
			_, _, err := a.ExecuteCapture(ctx, csRequest(false, "INSERT INTO foo VALUES(1, 'x')"), false)
			return err
		},
		"apply": func() error { return b.ApplyChangeset(ctx, cs) },
	} {
		for n := 1; ; n++ {
			fired, restore := failMallocAt(n)
			err := run()
			restore()
			if !*fired {
				if err != nil {
					t.Fatalf("%s: unexpected error without a fault: %s", name, err)
				}
				if n == 1 {
					t.Fatalf("%s: never allocated", name)
				}
				break
			}
			if err == nil || errors.Is(err, ErrNotCapturable) {
				t.Fatalf("%s: allocation %d failed but error is %v", name, n, err)
			}
		}
	}
	// A capture never commits, and the changeset was applied exactly once, by
	// the final fault-free run.
	if dump(t, a) != "foo: []\n" || dump(t, b) != "foo: [[1,1,\"x\"]]\n" {
		t.Fatalf("unexpected contents:\n%s\n%s", dump(t, a), dump(t, b))
	}
}

func Test_Changeset_SessionCallsFail(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	failing := sqlite3.SQLITE_ERROR
	for name, inject := range map[string]func() func(){
		"create": func() func() {
			real := cSessionCreate
			cSessionCreate = func(*libc.TLS, uintptr, uintptr, uintptr) int32 { return int32(failing) }
			return func() { cSessionCreate = real }
		},
		"object_config": func() func() {
			real := cSessionObjectConfig
			cSessionObjectConfig = func(*libc.TLS, uintptr, int32, uintptr) int32 { return int32(failing) }
			return func() { cSessionObjectConfig = real }
		},
		"attach": func() func() {
			real := cSessionAttach
			cSessionAttach = func(*libc.TLS, uintptr, uintptr) int32 { return int32(failing) }
			return func() { cSessionAttach = real }
		},
		"changeset": func() func() {
			real := cSessionChangeset
			cSessionChangeset = func(*libc.TLS, uintptr, uintptr, uintptr) int32 { return int32(failing) }
			return func() { cSessionChangeset = real }
		},
	} {
		restore := inject()
		_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(1)"), false)
		restore()
		var se *SQLiteError
		if !errors.As(err, &se) || errors.Is(err, ErrNotCapturable) {
			t.Fatalf("%s: expected an SQLite error, got %v", name, err)
		}
	}
	if r := mustQuery(a, "SELECT count(*) FROM foo"); r[0].Values[0].Parameters[0].GetI() != 0 {
		t.Fatal("a failed capture changed the database")
	}
	mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1)"))
}

func Test_Changeset_DBConfigFails(t *testing.T) {
	a, b := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1)"))
	real := cDBConfig
	defer func() { cDBConfig = real }()

	for _, failAt := range []int{1, 2} {
		calls := 0
		cDBConfig = func(tls *libc.TLS, db uintptr, op int32, va uintptr) int32 {
			calls++
			if calls == failAt {
				return sqlite3.SQLITE_ERROR
			}
			return real(tls, db, op, va)
		}
		err := b.ApplyChangeset(context.Background(), cs)
		cDBConfig = real
		if err == nil {
			t.Fatalf("db_config call %d failed but apply succeeded", failAt)
		}
		// The first call turns triggers off, before anything is applied. The
		// second turns them back on: a connection left with triggers off is
		// not safe to keep writing with.
		var fatal *FatalError
		if got := errors.As(err, &fatal); got != (failAt == 2) {
			t.Fatalf("db_config call %d: fatal=%v (%v)", failAt, got, err)
		}
		if failAt == 1 && dump(t, b) != "foo: []\n" {
			t.Fatal("changeset applied although triggers could not be turned off")
		}
	}
}

func Test_Changeset_SchemaVersion(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")

	// DDL that gets past the keyword check is still caught, by the authorizer.
	prefixes := notCapturablePrefixes
	notCapturablePrefixes = nil
	defer func() { notCapturablePrefixes = prefixes }()
	req := csRequest(false, "INSERT INTO foo VALUES(1)", "CREATE TABLE bar (id INTEGER)")
	if !CapturableRequest(req) {
		t.Fatal("test precondition: the request should pass the keyword check")
	}
	if _, _, err := a.ExecuteCapture(context.Background(), req, false); !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("expected ErrNotCapturable, got %v", err)
	}
	if r := mustQuery(a, "SELECT count(*) FROM sqlite_master WHERE name='bar'"); r[0].Values[0].Parameters[0].GetI() != 0 {
		t.Fatal("DDL was left in the database")
	}
}

func Test_Changeset_EndsTransaction(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	mustExecute(a, "INSERT INTO foo VALUES(1)")
	// OR ROLLBACK ends the transaction the capture runs in, and gets past the
	// keyword check.
	_, _, err := a.ExecuteCapture(context.Background(), csRequest(false, "INSERT OR ROLLBACK INTO foo VALUES(1)"), false)
	if !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("expected ErrNotCapturable, got %v", err)
	}
	mustExecute(a, "INSERT INTO foo VALUES(2)")
}

func Test_Changeset_RunError(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	conn, err := a.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	boom := errors.New("boom")
	res, cs, err := conn.capture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(1)"),
		func(*command.Request) ([]*command.ExecuteQueryResponse, error) { return nil, boom })
	if !errors.Is(err, boom) || res != nil || cs != nil {
		t.Fatalf("expected only the run error, got %v %v %v", res, cs, err)
	}
}

func Test_Changeset_BeginFails(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	conn, err := a.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.exec(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	defer conn.exec(context.Background(), "ROLLBACK")
	// A transaction is already open, so BEGIN IMMEDIATE is refused.
	_, _, err = conn.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(1)"), false)
	if err == nil || errors.Is(err, ErrNotCapturable) {
		t.Fatalf("expected a BEGIN error, got %v", err)
	}
}

func Test_Changeset_ClosedConnection(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1)"))
	conn, err := a.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if err := conn.ApplyChangeset(cs); err == nil {
		t.Fatal("apply on a closed connection succeeded")
	}
	if _, _, err := conn.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(2)"), false); err == nil {
		t.Fatal("capture on a closed connection succeeded")
	}

	// A failure to take the raw handles is reported as it is, and nothing runs.
	boom := errors.New("boom")
	real := connHandles
	defer func() { connHandles = real }()
	connHandles = func(*Conn) (rawConn, error) { return rawConn{}, boom }
	live, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	res, cs2, err := live.ExecuteCapture(context.Background(), csRequest(false, "INSERT INTO foo VALUES(2)"), false)
	if !errors.Is(err, boom) || res != nil || cs2 != nil {
		t.Fatalf("expected only the raw handles error, got %v %v %v", res, cs2, err)
	}
	if got := dump(t, live); got != "foo: []\n" {
		t.Fatalf("unexpected contents %s", got)
	}
}

func Test_Changeset_WriterBusy(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	_, cs := mustCapture(t, a, csRequest(false, "INSERT INTO foo VALUES(1)"))
	held, err := a.AcquireWriter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req := csRequest(false, "INSERT INTO foo VALUES(2)")
	if _, _, err := a.ExecuteCapture(ctx, req, false); err == nil {
		t.Fatal("ExecuteCapture got the writer while it was held")
	}
	if _, _, err := a.RequestCapture(ctx, req, false); err == nil {
		t.Fatal("RequestCapture got the writer while it was held")
	}
	if err := a.ApplyChangeset(ctx, cs); err == nil {
		t.Fatal("ApplyChangeset got the writer while it was held")
	}
}

func Test_Changeset_EmptyApplyAllocatesNothing(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	real := cMalloc
	defer func() { cMalloc = real }()
	cMalloc = func(*libc.TLS, types.Size_t) uintptr {
		t.Fatal("an empty changeset must not reach the C library")
		return 0
	}
	for _, empty := range [][]byte{nil, {}} {
		if err := a.ApplyChangeset(context.Background(), empty); err != nil {
			t.Fatal(err)
		}
	}
}

func Test_Changeset_NothingChangedIsNil(t *testing.T) {
	a, _ := newPair(t, "CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	for _, sql := range []string{"SELECT 1", "UPDATE foo SET id = id WHERE 0"} {
		_, cs := mustCapture(t, a, csRequest(false, sql))
		if cs != nil {
			t.Fatalf("%q: expected a nil changeset, got %d bytes", sql, len(cs))
		}
	}
}
