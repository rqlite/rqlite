// Package sqlite3 adapts modernc SQLite to the subset of the former driver used
// by rqlite. This is an experimental adapter pinned to modernc v1.61.0.
package sqlite3

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"unsafe"

	"modernc.org/libc"
	modern "modernc.org/sqlite"
	lib "modernc.org/sqlite/lib"
)

type ErrNo int
type ErrNoExtended int
type Error struct {
	Code         ErrNo
	ExtendedCode ErrNoExtended
	SystemErrno  syscall.Errno
	message      string
}

func (e Error) Error() string {
	if e.message != "" {
		return e.message
	}
	return modern.ErrorCodeString[int(e.Code)]
}

const (
	ErrError                ErrNo         = lib.SQLITE_ERROR
	ErrBusy                 ErrNo         = lib.SQLITE_BUSY
	ErrLocked               ErrNo         = lib.SQLITE_LOCKED
	ErrReadonly             ErrNo         = lib.SQLITE_READONLY
	ErrFull                 ErrNo         = lib.SQLITE_FULL
	ErrIoErr                ErrNo         = lib.SQLITE_IOERR
	ErrNomem                ErrNo         = lib.SQLITE_NOMEM
	ErrCorrupt              ErrNo         = lib.SQLITE_CORRUPT
	ErrNotADB               ErrNo         = lib.SQLITE_NOTADB
	ErrCantOpen             ErrNo         = lib.SQLITE_CANTOPEN
	ErrPerm                 ErrNo         = lib.SQLITE_PERM
	ErrNoLFS                ErrNo         = lib.SQLITE_NOLFS
	ErrProtocol             ErrNo         = lib.SQLITE_PROTOCOL
	ErrInternal             ErrNo         = lib.SQLITE_INTERNAL
	ErrMisuse               ErrNo         = lib.SQLITE_MISUSE
	ErrConstraint           ErrNo         = lib.SQLITE_CONSTRAINT
	ErrConstraintPrimaryKey ErrNoExtended = lib.SQLITE_CONSTRAINT_PRIMARYKEY
	ErrIoErrWrite           ErrNoExtended = lib.SQLITE_IOERR_WRITE
	ErrIoErrRead            ErrNoExtended = lib.SQLITE_IOERR_READ
	ErrIoErrFsync           ErrNoExtended = lib.SQLITE_IOERR_FSYNC
	SQLITE_INSERT                         = lib.SQLITE_INSERT
	SQLITE_UPDATE                         = lib.SQLITE_UPDATE
	SQLITE_DELETE                         = lib.SQLITE_DELETE
	SQLITE_LIMIT_ATTACHED                 = lib.SQLITE_LIMIT_ATTACHED
	SQLITE_LIMIT_VDBE_OP                  = lib.SQLITE_LIMIT_VDBE_OP
)

type SQLitePreUpdateData = modern.SQLitePreUpdateData
type SQLiteDriver struct {
	Extensions  []string
	ConnectHook func(*SQLiteConn) error
}
type SQLiteConn struct {
	driver.Conn
	tls *libc.TLS
	db  uintptr
}

func init() { sql.Register("sqlite3", &SQLiteDriver{}) }
func Version() (string, string, int) {
	return lib.SQLITE_VERSION, "modernc.org/sqlite v1.61.0", lib.SQLITE_VERSION_NUMBER
}
func (d *SQLiteDriver) Open(dsn string) (driver.Conn, error) {
	if len(d.Extensions) > 0 {
		return nil, fmt.Errorf("modernc experiment: native SQLite extensions are unsupported")
	}
	raw, err := (&modern.Driver{}).Open(dsn)
	if err != nil {
		return nil, err
	}
	// modernc does not export its connection handle. Read these named fields
	// once, rather than depending on offsets; upgrades require revalidation.
	v := reflect.ValueOf(raw).Elem()
	c := &SQLiteConn{Conn: raw, db: uintptr(v.FieldByName("db").Uint()), tls: (*libc.TLS)(unsafe.Pointer(v.FieldByName("tls").Pointer()))}
	if d.ConnectHook != nil {
		if err = d.ConnectHook(c); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}
func (c *SQLiteConn) convert(err error) error {
	if err == nil {
		return nil
	}
	var e *modern.Error
	if !errors.As(err, &e) {
		return err
	}
	return Error{Code: ErrNo(e.Code() & 255), ExtendedCode: ErrNoExtended(e.Code()), SystemErrno: syscall.Errno(lib.Xsqlite3_system_errno(c.tls, c.db)), message: libc.GoString(lib.Xsqlite3_errmsg(c.tls, c.db))}
}
func (c *SQLiteConn) rc(rc int32) error {
	if rc == lib.SQLITE_OK {
		return nil
	}
	return Error{Code: ErrNo(rc & 255), ExtendedCode: ErrNoExtended(rc), message: libc.GoString(lib.Xsqlite3_errmsg(c.tls, c.db))}
}
func (c *SQLiteConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	r, e := c.Conn.(driver.ExecerContext).ExecContext(ctx, q, a)
	if r == nil && e == nil {
		r = emptyResult{}
	}
	return r, c.convert(e)
}
func (c *SQLiteConn) Exec(q string, a []driver.Value) (driver.Result, error) {
	n := make([]driver.NamedValue, len(a))
	for i, v := range a {
		n[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return c.ExecContext(context.Background(), q, n)
}
func (c *SQLiteConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	r, e := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, a)
	if e != nil {
		return nil, c.convert(e)
	}
	return &rows{Rows: r, c: c, ctx: ctx}, nil
}

type emptyResult struct{}

func (emptyResult) LastInsertId() (int64, error) { return 0, nil }
func (emptyResult) RowsAffected() (int64, error) { return 0, nil }

type rows struct {
	ctx context.Context
	driver.Rows
	c *SQLiteConn
}

func (r *rows) Next(v []driver.Value) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	return r.c.convert(r.Rows.Next(v))
}
func (r *rows) Close() error { return r.c.convert(r.Rows.Close()) }
func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	return r.Rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(i)
}
func (c *SQLiteConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *SQLiteConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	t, e := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
	if e != nil {
		return nil, c.convert(e)
	}
	return &tx{Tx: t, c: c}, nil
}

type tx struct {
	driver.Tx
	c *SQLiteConn
}

func (t *tx) Commit() error            { return t.c.convert(t.Tx.Commit()) }
func (t *tx) Rollback() error          { return t.c.convert(t.Tx.Rollback()) }
func (c *SQLiteConn) AutoCommit() bool { return lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 }
func (c *SQLiteConn) SetLimit(id, n int) int {
	return int(lib.Xsqlite3_limit(c.tls, c.db, int32(id), int32(n)))
}
func (c *SQLiteConn) DBConfigNoCkptOnClose() error {
	p := libc.Xmalloc(c.tls, 16)
	defer libc.Xfree(c.tls, p)
	return c.rc(lib.Xsqlite3_db_config(c.tls, c.db, lib.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, libc.VaList(p, int32(1), uintptr(0))))
}
func (c *SQLiteConn) LoadExtension(string, string) error {
	return fmt.Errorf("modernc experiment: native SQLite extensions are unsupported")
}
func (c *SQLiteConn) RegisterPreUpdateHook(f func(SQLitePreUpdateData)) {
	c.Conn.(modern.HookRegisterer).RegisterPreUpdateHook(modern.PreUpdateHookFn(f))
}
func (c *SQLiteConn) RegisterCommitHook(f func() int) {
	c.Conn.(modern.HookRegisterer).RegisterCommitHook(func() int32 { return int32(f()) })
}
func (c *SQLiteConn) RegisterRollbackHook(f func()) {
	c.Conn.(modern.HookRegisterer).RegisterRollbackHook(modern.RollbackHookFn(f))
}

// These statements are prepared by SQLite itself for metadata and read-only
// classification. Execution still goes through modernc's database/sql driver.
type SQLiteStmt struct {
	driver.Stmt
	c *SQLiteConn
	p uintptr
}

func (c *SQLiteConn) Prepare(q string) (driver.Stmt, error) {
	z, e := libc.CString(q)
	if e != nil {
		return nil, e
	}
	defer libc.Xfree(c.tls, z)
	out := libc.Xmalloc(c.tls, 8)
	defer libc.Xfree(c.tls, out)
	if e = c.rc(lib.Xsqlite3_prepare_v2(c.tls, c.db, z, -1, out, 0)); e != nil {
		return nil, e
	}
	p := *(*uintptr)(unsafe.Pointer(out))
	s, e := c.Conn.Prepare(q)
	if e != nil {
		lib.Xsqlite3_finalize(c.tls, p)
		return nil, c.convert(e)
	}
	return &SQLiteStmt{Stmt: s, c: c, p: p}, nil
}
func (s *SQLiteStmt) Close() error {
	lib.Xsqlite3_finalize(s.c.tls, s.p)
	return s.c.convert(s.Stmt.Close())
}
func (s *SQLiteStmt) Readonly() bool { return lib.Xsqlite3_stmt_readonly(s.c.tls, s.p) != 0 }
func (s *SQLiteStmt) ColumnTableName(i int) string {
	return libc.GoString(lib.Xsqlite3_column_table_name(s.c.tls, s.p, int32(i)))
}

type backup struct {
	c *SQLiteConn
	p uintptr
}

func (c *SQLiteConn) Backup(dst string, src *SQLiteConn, source string) (*backup, error) {
	d, e := libc.CString(dst)
	if e != nil {
		return nil, e
	}
	defer libc.Xfree(c.tls, d)
	s, e := libc.CString(source)
	if e != nil {
		return nil, e
	}
	defer libc.Xfree(c.tls, s)
	p := lib.Xsqlite3_backup_init(c.tls, c.db, d, src.db, s)
	if p == 0 {
		return nil, c.rc(lib.Xsqlite3_errcode(c.tls, c.db))
	}
	return &backup{c: c, p: p}, nil
}
func (b *backup) Step(n int) (bool, error) {
	rc := lib.Xsqlite3_backup_step(b.c.tls, b.p, int32(n))
	if rc == lib.SQLITE_DONE {
		return true, nil
	}
	if rc == lib.SQLITE_BUSY || rc == lib.SQLITE_LOCKED {
		return false, nil
	}
	return false, b.c.rc(rc)
}
func (b *backup) Finish() error { return b.c.rc(lib.Xsqlite3_backup_finish(b.c.tls, b.p)) }
