package db

import (
	"sort"
	"sync"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// A SQLite authorizer is installed on the connection for the duration of a
// capture. It is the authority on what a captured request may do; the keyword
// check in CapturableRequest is only a fast path in front of it.
//
// While a capture runs inside its BEGIN IMMEDIATE transaction, the authorizer
// refuses everything that could act outside that transaction or that the session
// extension does not record: transaction control, savepoints, PRAGMA, ATTACH,
// DETACH and every kind of DDL. It also refuses every statement prepared after
// the transaction has ended (INSERT OR ROLLBACK, RAISE(ROLLBACK) in a trigger),
// since such a statement would commit on this node alone. Statements of a SQL
// string are prepared one after the other, so this covers the tail of a string
// as well as later statements of the request.
//
// The authorizer also records which tables of main the request writes, and
// refuses nothing for them; a write outside main (a TEMP table, an attached
// database) is refused because the session records main only.
type authCtx struct {
	mu     sync.Mutex
	denied bool
	tables map[string]struct{}
}

var authorizers = struct {
	sync.RWMutex
	m map[uintptr]*authCtx
}{m: make(map[uintptr]*authCtx)}

// installAuthorizer makes the capture authorizer the one of the connection.
func (r rawConn) installAuthorizer() *authCtx {
	a := &authCtx{tables: make(map[string]struct{})}
	authorizers.Lock()
	authorizers.m[r.db] = a
	authorizers.Unlock()
	sqlite3.Xsqlite3_set_authorizer(r.tls, r.db, cFuncPointer(authorizerCallback), r.db)
	return a
}

// removeAuthorizer restores the connection's authorizer-free state.
func (r rawConn) removeAuthorizer() {
	sqlite3.Xsqlite3_set_authorizer(r.tls, r.db, 0, 0)
	authorizers.Lock()
	delete(authorizers.m, r.db)
	authorizers.Unlock()
}

// refused reports whether the authorizer denied anything.
func (a *authCtx) refused() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.denied
}

// writtenTables returns the main-schema tables the request writes, sorted.
func (a *authCtx) writtenTables() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.tables))
	for t := range a.tables {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func authorizerCallback(tls *libc.TLS, pArg uintptr, op int32, z1, _, z3, _ uintptr) int32 {
	authorizers.RLock()
	a := authorizers.m[pArg]
	authorizers.RUnlock()
	if a == nil {
		return sqlite3.SQLITE_OK
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// The capture transaction is gone: nothing more may run.
	if sqlite3.Xsqlite3_get_autocommit(tls, pArg) != 0 {
		a.denied = true
		return sqlite3.SQLITE_DENY
	}
	// The write path itself reads column metadata (generated columns); these two
	// pragmas only read.
	if op == sqlite3.SQLITE_PRAGMA {
		switch libc.GoString(z1) {
		case "table_info", "table_xinfo":
			return sqlite3.SQLITE_OK
		}
	}
	switch op {
	case sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT, sqlite3.SQLITE_PRAGMA,
		sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH,
		sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_CREATE_TEMP_INDEX,
		sqlite3.SQLITE_CREATE_TEMP_TABLE, sqlite3.SQLITE_CREATE_TEMP_TRIGGER, sqlite3.SQLITE_CREATE_TEMP_VIEW,
		sqlite3.SQLITE_CREATE_TRIGGER, sqlite3.SQLITE_CREATE_VIEW, sqlite3.SQLITE_CREATE_VTABLE,
		sqlite3.SQLITE_DROP_INDEX, sqlite3.SQLITE_DROP_TABLE, sqlite3.SQLITE_DROP_TEMP_INDEX,
		sqlite3.SQLITE_DROP_TEMP_TABLE, sqlite3.SQLITE_DROP_TEMP_TRIGGER, sqlite3.SQLITE_DROP_TEMP_VIEW,
		sqlite3.SQLITE_DROP_TRIGGER, sqlite3.SQLITE_DROP_VIEW, sqlite3.SQLITE_DROP_VTABLE,
		sqlite3.SQLITE_ALTER_TABLE, sqlite3.SQLITE_REINDEX, sqlite3.SQLITE_ANALYZE:
		a.denied = true
		return sqlite3.SQLITE_DENY
	case sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE:
		if libc.GoString(z3) != "main" {
			a.denied = true
			return sqlite3.SQLITE_DENY
		}
		a.tables[libc.GoString(z1)] = struct{}{}
	}
	return sqlite3.SQLITE_OK
}
