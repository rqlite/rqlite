package db

import (
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/rqlite/rqlite/v10/db/querylog"
	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

// rawConn is the SQLite handle and libc thread state behind one
// modernc.org/sqlite driver connection. The driver does not export its
// connection type, so the two fields are recovered by reflection. They give
// access to the C-level calls the driver does not wrap: autocommit state,
// statement read-only checks, the online backup API, the update hook,
// tracing, limits and db_config.
type rawConn struct {
	tls *libc.TLS
	db  uintptr
}

// rawHandles returns the raw SQLite handles behind a driver connection.
func rawHandles(driverConn any) (rawConn, error) {
	v := reflect.ValueOf(driverConn)
	if v.Kind() != reflect.Pointer || v.Elem().Kind() != reflect.Struct {
		return rawConn{}, fmt.Errorf("unexpected driver connection type: %T", driverConn)
	}
	e := v.Elem()
	dbF := e.FieldByName("db")
	tlsF := e.FieldByName("tls")
	if !dbF.IsValid() || dbF.Kind() != reflect.Uintptr || !tlsF.IsValid() || tlsF.Kind() != reflect.Pointer {
		return rawConn{}, fmt.Errorf("driver connection %T lacks expected db/tls fields", driverConn)
	}
	return rawConn{
		tls: (*libc.TLS)(tlsF.UnsafePointer()),
		db:  uintptr(dbF.Uint()),
	}, nil
}

// errstr converts a SQLite result code into a structured error, using the
// connection's last error message when it belongs to rc.
func (r rawConn) errstr(rc int32) error {
	ext := sqlite3.Xsqlite3_extended_errcode(r.tls, r.db)
	var msg string
	if ext&0xff == rc&0xff {
		msg = libc.GoString(sqlite3.Xsqlite3_errmsg(r.tls, r.db))
	} else {
		ext = rc
		msg = libc.GoString(sqlite3.Xsqlite3_errstr(r.tls, rc))
	}
	return &SQLiteError{
		Code:         rc & 0xff,
		ExtendedCode: ext,
		SystemErrno:  sqlite3.Xsqlite3_system_errno(r.tls, r.db),
		msg:          msg,
	}
}

const ptrSize = types.Size_t(unsafe.Sizeof(uintptr(0)))

// cPtr converts a C address held in a uintptr into an unsafe.Pointer without
// tripping vet's unsafeptr check. The libc heap is not managed by the Go GC,
// so the address is stable.
func cPtr(p uintptr) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&p))
}

// cFuncPointer converts a Go function into a value the ccgo-translated SQLite
// library can call as a C function pointer. Copied from modernc.org/sqlite.
func cFuncPointer[T any](f T) uintptr {
	return *(*uintptr)(unsafe.Pointer(&struct{ f T }{f}))
}

// dbConfigNoCkptOnClose stops the connection checkpointing the WAL on close.
func (r rawConn) dbConfigNoCkptOnClose() error {
	const vaSlot = 8
	bp := libc.Xmalloc(r.tls, types.Size_t(2*vaSlot))
	if bp == 0 {
		return r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	rc := sqlite3.Xsqlite3_db_config(r.tls, r.db, sqlite3.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
		libc.VaList(bp, int32(1), uintptr(0)))
	if rc != sqlite3.SQLITE_OK {
		return r.errstr(rc)
	}
	return nil
}

// setLimit sets a run-time limit on the connection, returning the old value.
func (r rawConn) setLimit(id, newVal int) int {
	return int(sqlite3.Xsqlite3_limit(r.tls, r.db, int32(id), int32(newVal)))
}

// autocommit returns whether the connection is in autocommit mode, i.e. has
// no open transaction.
func (r rawConn) autocommit() bool {
	return sqlite3.Xsqlite3_get_autocommit(r.tls, r.db) != 0
}

// stmtReadOnly prepares query and reports whether SQLite considers it read-only.
func (r rawConn) stmtReadOnly(query string) (bool, error) {
	zSQL, err := libc.CString(query)
	if err != nil {
		return false, err
	}
	defer libc.Xfree(r.tls, zSQL)

	bp := libc.Xmalloc(r.tls, 2*ptrSize)
	if bp == 0 {
		return false, r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	*(*uintptr)(cPtr(bp)) = 0

	rc := sqlite3.Xsqlite3_prepare_v2(r.tls, r.db, zSQL, -1, bp, bp+uintptr(ptrSize))
	if rc != sqlite3.SQLITE_OK {
		return false, r.errstr(rc)
	}
	pStmt := *(*uintptr)(cPtr(bp))
	if pStmt == 0 {
		// Whitespace or a comment: nothing to run, so nothing to write.
		return true, nil
	}
	ro := sqlite3.Xsqlite3_stmt_readonly(r.tls, pStmt) != 0
	sqlite3.Xsqlite3_finalize(r.tls, pStmt)
	return ro, nil
}

// backupFrom copies the main database behind src into the main database
// behind r using the SQLite online backup API.
func (r rawConn) backupFrom(src rawConn) error {
	zMain, err := libc.CString("main")
	if err != nil {
		return err
	}
	defer libc.Xfree(r.tls, zMain)

	bk := sqlite3.Xsqlite3_backup_init(r.tls, r.db, zMain, src.db, zMain)
	if bk == 0 {
		return r.errstr(sqlite3.Xsqlite3_errcode(r.tls, r.db))
	}

	for {
		rc := sqlite3.Xsqlite3_backup_step(r.tls, bk, -1)
		if rc == sqlite3.SQLITE_DONE {
			stats.Add(numBackupStepDones, 1)
			break
		}
		if rc != sqlite3.SQLITE_OK && rc != sqlite3.SQLITE_BUSY && rc != sqlite3.SQLITE_LOCKED {
			stats.Add(numBackupStepErrors, 1)
			err := r.errstr(rc)
			sqlite3.Xsqlite3_backup_finish(r.tls, bk)
			return err
		}
		stats.Add(numBackupSleeps, 1)
		time.Sleep(bkDelay)
	}
	if rc := sqlite3.Xsqlite3_backup_finish(r.tls, bk); rc != sqlite3.SQLITE_OK {
		return r.errstr(rc)
	}
	return nil
}

// Update hooks, keyed by database handle. The driver registers the preupdate,
// commit and rollback hooks itself but has no update hook, so it is installed
// here with the same trampoline technique.
var updateHooks = struct {
	sync.RWMutex
	m map[uintptr]func(int, string, string, int64)
}{m: make(map[uintptr]func(int, string, string, int64))}

func (r rawConn) registerUpdateHook(fn func(op int, dbName, tblName string, rowID int64)) {
	updateHooks.Lock()
	if fn == nil {
		delete(updateHooks.m, r.db)
	} else {
		updateHooks.m[r.db] = fn
	}
	updateHooks.Unlock()
	if fn == nil {
		sqlite3.Xsqlite3_update_hook(r.tls, r.db, 0, 0)
		return
	}
	sqlite3.Xsqlite3_update_hook(r.tls, r.db, cFuncPointer(updateHookTrampoline), r.db)
}

func updateHookTrampoline(_ *libc.TLS, pArg uintptr, op int32, zDb, zTbl uintptr, rowID int64) {
	updateHooks.RLock()
	fn := updateHooks.m[pArg]
	updateHooks.RUnlock()
	if fn == nil {
		return
	}
	fn(int(op), libc.GoString(zDb), libc.GoString(zTbl), rowID)
}

// Trace hooks, keyed by database handle.
var traceHooks = struct {
	sync.RWMutex
	m map[uintptr]func(querylog.TraceInfo) int
}{m: make(map[uintptr]func(querylog.TraceInfo) int)}

// setTrace installs fn as the sqlite3_trace_v2 callback for the given event mask.
func (r rawConn) setTrace(mask uint32, fn func(querylog.TraceInfo) int) error {
	traceHooks.Lock()
	if fn == nil {
		delete(traceHooks.m, r.db)
	} else {
		traceHooks.m[r.db] = fn
	}
	traceHooks.Unlock()
	var rc int32
	if fn == nil {
		rc = sqlite3.Xsqlite3_trace_v2(r.tls, r.db, 0, 0, 0)
	} else {
		rc = sqlite3.Xsqlite3_trace_v2(r.tls, r.db, mask, cFuncPointer(traceTrampoline), r.db)
	}
	if rc != sqlite3.SQLITE_OK {
		return r.errstr(rc)
	}
	return nil
}

func traceTrampoline(tls *libc.TLS, t uint32, pCtx, p, x uintptr) int32 {
	traceHooks.RLock()
	fn := traceHooks.m[pCtx]
	traceHooks.RUnlock()
	if fn == nil {
		return 0
	}
	info := querylog.TraceInfo{
		EventCode:  t,
		ConnHandle: pCtx,
		StmtHandle: p,
	}
	switch t {
	case querylog.TraceStmt:
		info.StmtOrTrigger = libc.GoString(x)
		if exp := sqlite3.Xsqlite3_expanded_sql(tls, p); exp != 0 {
			info.ExpandedSQL = libc.GoString(exp)
			sqlite3.Xsqlite3_free(tls, exp)
		}
	case querylog.TraceProfile:
		if x != 0 {
			info.RunTimeNanosec = *(*int64)(cPtr(x))
		}
	}
	return int32(fn(info))
}
