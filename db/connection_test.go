package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
)

// Test_ConnectionFactory_Pool tests that a pool created from a factory supports
// normal database operations, and that its connections are Connections.
func Test_ConnectionFactory_Pool(t *testing.T) {
	f, db := mustOpenFactoryPool(t, ModeReadWrite, &DriverConfig{})

	if _, err := db.Exec("INSERT INTO foo(name) VALUES(?)", "fiona"); err != nil {
		t.Fatalf("failed to insert: %s", err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to begin transaction: %s", err)
	}
	stmt, err := tx.Prepare("INSERT INTO foo(name) VALUES(?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %s", err)
	}
	if _, err := stmt.Exec("declan"); err != nil {
		t.Fatalf("failed to execute prepared statement: %s", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %s", err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM foo").Scan(&n); err != nil || n != 2 {
		t.Fatalf("row count: got %d, error %v, want 2", n, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping: %s", err)
	}

	conn := mustConn(t, db)
	defer conn.Close()
	if err := conn.Raw(func(driverConn any) error {
		c, ok := driverConn.(*Connection)
		if !ok {
			t.Fatalf("driver connection is a %T, want a *Connection", driverConn)
		}
		if c.factory != f {
			t.Fatalf("connection does not refer to the factory which opened it")
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to access driver connection: %s", err)
	}
}

// Test_ConnectionFactory_Config tests that the DriverConfig is applied to every
// connection, whether read-write or read-only.
func Test_ConnectionFactory_Config(t *testing.T) {
	for _, readOnly := range []bool{ModeReadWrite, ModeReadOnly} {
		_, db := mustOpenFactoryPool(t, readOnly, &DriverConfig{ForeignKeys: true})
		for _, conn := range mustConns(t, db, 3) {
			if n := mustPragmaInt(t, conn, "wal_autocheckpoint"); n != 0 {
				t.Fatalf("read-only %v: autocheckpoint is %d, want 0", readOnly, n)
			}
			if n := mustPragmaInt(t, conn, "foreign_keys"); n != 1 {
				t.Fatalf("read-only %v: foreign_keys is %d, want 1", readOnly, n)
			}
		}
	}
}

// Test_ConnectionFactory_ReadOnly tests that the DSN alone determines whether a
// factory's connections are read-only.
func Test_ConnectionFactory_ReadOnly(t *testing.T) {
	_, db := mustOpenFactoryPool(t, ModeReadOnly, &DriverConfig{})
	if _, err := db.Exec("INSERT INTO foo(name) VALUES('fiona')"); err == nil ||
		!strings.Contains(err.Error(), "readonly") {
		t.Fatalf("expected write on read-only connection to fail, got %v", err)
	}
}

// Test_ConnectionFactory_Tracking tests that a factory tracks the connections
// it has opened, and stops tracking a connection when it is closed.
func Test_ConnectionFactory_Tracking(t *testing.T) {
	f, db := mustOpenFactoryPool(t, ModeReadWrite, &DriverConfig{})
	db.SetMaxIdleConns(0) // Close connections as soon as they are released.
	if n := f.NumConnections(); n != 0 {
		t.Fatalf("got %d connections with idle pool, want 0", n)
	}

	conns := mustConns(t, db, 3)
	if n := f.NumConnections(); n != 3 {
		t.Fatalf("got %d connections, want 3", n)
	}
	conns[0].Close()
	if n := f.NumConnections(); n != 2 {
		t.Fatalf("got %d connections after closing one, want 2", n)
	}

	// A connection discarded by the pool is closed, and so no longer tracked.
	if err := conns[1].Raw(func(any) error { return driver.ErrBadConn }); err != driver.ErrBadConn {
		t.Fatalf("failed to discard connection: %v", err)
	}
	if n := f.NumConnections(); n != 1 {
		t.Fatalf("got %d connections after discarding one, want 1", n)
	}

	// Closing the pool closes a connection which is in use once it is released.
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close pool: %s", err)
	}
	conns[2].Close()
	if n := f.NumConnections(); n != 0 {
		t.Fatalf("got %d connections after closing pool, want 0", n)
	}
}

// Test_ConnectionFactory_ConnectError tests that a connection which cannot be
// opened is not tracked.
func Test_ConnectionFactory_ConnectError(t *testing.T) {
	dsn := MakeDSN(filepath.Join(t.TempDir(), "non-existent", "db.sqlite"), ModeReadWrite, false, true)
	f := NewConnectionFactory(dsn, &DriverConfig{})
	if conn, err := f.Connect(context.Background()); err == nil {
		conn.Close()
		t.Fatalf("expected error opening connection")
	}
	if n := f.NumConnections(); n != 0 {
		t.Fatalf("got %d connections after failed connect, want 0", n)
	}
}

// Test_ConnectionFactory_Settings tests that changing a setting changes it on
// all current connections, and on connections opened afterwards, regardless of
// whether the connections are read-write or read-only.
func Test_ConnectionFactory_Settings(t *testing.T) {
	for _, readOnly := range []bool{ModeReadWrite, ModeReadOnly} {
		f, db := mustOpenFactoryPool(t, readOnly, &DriverConfig{})
		check := func(conns []*sql.Conn, wantBusy int, wantSync SynchronousMode) {
			t.Helper()
			for i, conn := range conns {
				if n := mustPragmaInt(t, conn, "busy_timeout"); n != wantBusy {
					t.Fatalf("read-only %v, connection %d: busy_timeout is %d, want %d", readOnly, i, n, wantBusy)
				}
				if n := mustPragmaInt(t, conn, "synchronous"); n != int(wantSync) {
					t.Fatalf("read-only %v, connection %d: synchronous is %d, want %d", readOnly, i, n, wantSync)
				}
			}
		}

		// Connections are untouched until a setting is set.
		conns := mustConns(t, db, 3)
		check(conns, 5000, SynchronousOff)

		// Current connections.
		if err := f.SetBusyTimeout(1234); err != nil {
			t.Fatalf("failed to set busy timeout: %s", err)
		}
		if err := f.SetSynchronousMode(SynchronousFull); err != nil {
			t.Fatalf("failed to set synchronous mode: %s", err)
		}
		check(conns, 1234, SynchronousFull)

		// Future connections, including a replacement for a discarded one.
		conns[0].Raw(func(any) error { return driver.ErrBadConn })
		conns = append(conns[1:], mustConns(t, db, 3)...)
		check(conns, 1234, SynchronousFull)

		if err := f.SetBusyTimeout(-1); err == nil {
			t.Fatalf("expected error setting negative busy timeout")
		}
		check(conns, 1234, SynchronousFull)
	}
}

// Test_ConnectionFactory_SettingsConnectionInUse tests changing settings while
// a connection has a transaction open.
func Test_ConnectionFactory_SettingsConnectionInUse(t *testing.T) {
	f, db := mustOpenFactoryPool(t, ModeReadWrite, &DriverConfig{})
	conn := mustConn(t, db)
	mustConnExec(t, conn, "BEGIN")
	mustConnExec(t, conn, "INSERT INTO foo(name) VALUES('fiona')")

	if err := f.SetBusyTimeout(1234); err != nil {
		t.Fatalf("failed to set busy timeout: %s", err)
	}

	// SQLite does not allow the synchronous mode of a connection to be changed
	// while it has a transaction open. The connection is left in its previous
	// mode, but connections opened afterwards have the new mode.
	if err := f.SetSynchronousMode(SynchronousFull); err == nil {
		t.Fatalf("expected error setting synchronous mode of connection with open transaction")
	}
	mustConnExec(t, conn, "COMMIT")

	if n := mustPragmaInt(t, conn, "busy_timeout"); n != 1234 {
		t.Fatalf("busy_timeout is %d, want 1234", n)
	}
	if n := mustPragmaInt(t, conn, "synchronous"); n != int(SynchronousOff) {
		t.Fatalf("synchronous is %d, want %d", n, SynchronousOff)
	}
	if n := mustPragmaInt(t, mustConn(t, db), "synchronous"); n != int(SynchronousFull) {
		t.Fatalf("synchronous of new connection is %d, want %d", n, SynchronousFull)
	}

	// With no transaction open the mode can be set.
	if err := f.SetSynchronousMode(SynchronousFull); err != nil {
		t.Fatalf("failed to set synchronous mode: %s", err)
	}
	if n := mustPragmaInt(t, conn, "synchronous"); n != int(SynchronousFull) {
		t.Fatalf("synchronous is %d, want %d", n, SynchronousFull)
	}
}

// Test_ConnectionFactory_Hooks tests that hooks are registered on, and removed
// from, all current connections and connections opened afterwards.
func Test_ConnectionFactory_Hooks(t *testing.T) {
	f, db := mustOpenFactoryPool(t, ModeReadWrite, &DriverConfig{})
	conns := mustConns(t, db, 2)

	var preupdates, updates, commits, rollbacks int
	register := func() {
		f.RegisterPreUpdateHook(func(sqlite3.SQLitePreUpdateData) { preupdates++ })
		f.RegisterUpdateHook(func(int, string, string, int64) { updates++ })
		f.RegisterCommitHook(func() int { commits++; return 0 })
		f.RegisterRollbackHook(func() { rollbacks++ })
	}
	check := func(conns []*sql.Conn, want int) {
		t.Helper()
		for i, conn := range conns {
			preupdates, updates, commits, rollbacks = 0, 0, 0, 0
			mustConnExec(t, conn, "INSERT INTO foo(name) VALUES('fiona')")
			mustConnExec(t, conn, "BEGIN")
			mustConnExec(t, conn, "INSERT INTO foo(name) VALUES('declan')")
			mustConnExec(t, conn, "ROLLBACK")
			if preupdates != 2*want || updates != 2*want || commits != want || rollbacks != want {
				t.Fatalf("connection %d: preupdates=%d, updates=%d, commits=%d, rollbacks=%d, want %d, %d, %d, %d",
					i, preupdates, updates, commits, rollbacks, 2*want, 2*want, want, want)
			}
		}
	}

	check(conns, 0)

	// Current connections.
	register()
	check(conns, 1)

	// Future connections, including a replacement for a discarded one.
	conns[0].Raw(func(any) error { return driver.ErrBadConn })
	conns = append(conns[1:], mustConns(t, db, 2)...)
	check(conns, 1)

	// Removal, from current and future connections.
	f.RegisterPreUpdateHook(nil)
	f.RegisterUpdateHook(nil)
	f.RegisterCommitHook(nil)
	f.RegisterRollbackHook(nil)
	conns = append(conns, mustConns(t, db, 1)...)
	check(conns, 0)
}

// mustOpenFactoryPool creates a WAL-mode database containing a single table,
// and returns a factory for the database in the given mode, along with a pool
// created from that factory.
func mustOpenFactoryPool(t *testing.T, readOnly bool, cfg *DriverConfig) (*ConnectionFactory, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.sqlite")

	rw := sql.OpenDB(NewConnectionFactory(MakeDSN(path, ModeReadWrite, false, true), &DriverConfig{}))
	if _, err := rw.Exec("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %s", err)
	}
	t.Cleanup(func() { rw.Close() })

	f := NewConnectionFactory(MakeDSN(path, readOnly, false, true), cfg)
	db := sql.OpenDB(f)
	t.Cleanup(func() { db.Close() })
	return f, db
}

// mustConns returns n connections from the pool. Since each is held open, the
// pool must use n distinct connections.
func mustConns(t *testing.T, db *sql.DB, n int) []*sql.Conn {
	t.Helper()
	conns := make([]*sql.Conn, n)
	for i := range conns {
		conns[i] = mustConn(t, db)
	}
	return conns
}

func mustConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to get connection: %s", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func mustConnExec(t *testing.T, conn *sql.Conn, stmt string) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
		t.Fatalf("failed to execute %q: %s", stmt, err)
	}
}

func mustPragmaInt(t *testing.T, conn *sql.Conn, pragma string) int {
	t.Helper()
	var n int
	if err := conn.QueryRowContext(context.Background(), "PRAGMA "+pragma).Scan(&n); err != nil {
		t.Fatalf("failed to read PRAGMA %s: %s", pragma, err)
	}
	return n
}
