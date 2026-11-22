package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unsafe"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrNotCapturable is returned by ExecuteCapture and RequestCapture when the
// request cannot be replicated as a changeset. Nothing has been changed in the
// database, and the caller must run the request through the statement path.
var ErrNotCapturable = errors.New("request cannot be replicated as a changeset")

// ErrChangesetConflict is returned when a changeset cannot be applied because it
// conflicts with the contents of the database. The apply is aborted and rolled
// back, so the database is left as it was. It means this node's database differs
// from the one the changeset was recorded on.
var ErrChangesetConflict = errors.New("changeset conflicts with database contents")

// notCapturablePrefixes are the leading keywords of statements the session
// extension cannot record, or that cannot run inside the transaction used to
// record a changeset. DDL is not recorded by the session extension at all.
var notCapturablePrefixes = []string{
	"CREATE", "DROP", "ALTER", "REINDEX", "ANALYZE", "VACUUM", "PRAGMA",
	"ATTACH", "DETACH", "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE",
}

// CapturableRequest returns whether every statement of the request may be
// recorded as a changeset. It is a cheap syntactic check made before anything
// is run; statements that slip past it (DDL reached some other way) are caught
// by the schema-cookie check in ExecuteCapture.
//
// A request that asks for rollback-on-error is never capturable, since the
// ROLLBACK it issues would discard the transaction the changeset is recorded in.
func CapturableRequest(req *command.Request) bool {
	if req.RollbackOnError {
		return false
	}
	for _, s := range req.Statements {
		if s.Sql == "" {
			continue
		}
		if s.ForceStall {
			return false
		}
		kw := firstKeyword(s.Sql)
		for _, p := range notCapturablePrefixes {
			if kw == p {
				return false
			}
		}
	}
	return true
}

// firstKeyword returns the upper-cased first word of the statement, skipping
// whitespace and SQL comments.
func firstKeyword(sql string) string {
	for {
		sql = strings.TrimLeftFunc(sql, unicode.IsSpace)
		switch {
		case strings.HasPrefix(sql, "--"):
			i := strings.IndexByte(sql, '\n')
			if i < 0 {
				return ""
			}
			sql = sql[i+1:]
		case strings.HasPrefix(sql, "/*"):
			i := strings.Index(sql[2:], "*/")
			if i < 0 {
				return ""
			}
			sql = sql[i+4:]
		default:
			end := strings.IndexFunc(sql, func(r rune) bool { return !unicode.IsLetter(r) })
			if end < 0 {
				end = len(sql)
			}
			return strings.ToUpper(sql[:end])
		}
	}
}

// ExecuteCapture runs the write request in a transaction with a session
// attached to every table, rolls the transaction back, and returns the
// per-statement results together with the changeset that reproduces the
// request's effects. The database is not changed.
//
// The results (rows affected, last insert ID, errors) are those the statement
// path would return. A nil changeset with a nil error means the request changed
// nothing, so there is nothing to replicate. ErrNotCapturable is returned when
// the request cannot be replicated this way.
func (c *Conn) ExecuteCapture(ctx context.Context, req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, []byte, error) {
	return c.capture(ctx, req, func(r *command.Request) ([]*command.ExecuteQueryResponse, error) {
		return c.Execute(ctx, r, xTime)
	})
}

// RequestCapture is ExecuteCapture for requests that may also contain reads.
func (c *Conn) RequestCapture(ctx context.Context, req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, []byte, error) {
	return c.capture(ctx, req, func(r *command.Request) ([]*command.ExecuteQueryResponse, error) {
		return c.Request(ctx, r, xTime)
	})
}

func (c *Conn) capture(ctx context.Context, req *command.Request, run func(*command.Request) ([]*command.ExecuteQueryResponse, error)) (results []*command.ExecuteQueryResponse, changeset []byte, retErr error) {
	defer func() {
		if errors.Is(retErr, ErrNotCapturable) {
			stats.Add(numChangesetFallbacks, 1)
		} else if retErr == nil {
			stats.Add(numChangesetCaptures, 1)
		}
	}()
	if !CapturableRequest(req) {
		return nil, nil, ErrNotCapturable
	}

	var cookieBefore int64
	if err := c.conn.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&cookieBefore); err != nil {
		return nil, nil, classifyError(err)
	}

	// The whole request runs inside one transaction of ours, so the request
	// itself must not open another one. The all-or-nothing meaning of
	// req.Transaction is restored below.
	inner := &command.Request{
		Statements:     req.Statements,
		DbTimeout:      req.DbTimeout,
		QualifyColumns: req.QualifyColumns,
	}

	if err := c.exec(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, nil, classifyError(err)
	}
	defer func() {
		// The capture never commits. Roll back whatever is open, preserving
		// an earlier fatal error.
		if open, err := c.InTransaction(); err == nil && open {
			if err := c.exec(context.Background(), "ROLLBACK"); err != nil {
				preserveFatalError(&retErr, classifyError(err))
			}
		}
	}()

	// Raw holds the connection for the duration of its callback, so the
	// handles are taken out of it and used afterwards. This is safe because
	// the caller holds the only read-write connection.
	var raw rawConn
	if err := c.conn.Raw(func(driverConn any) (err error) {
		raw, err = rawHandles(driverConn)
		return err
	}); err != nil {
		return nil, nil, err
	}
	sess, err := raw.newSession()
	if err != nil {
		return nil, nil, err
	}
	defer sess.close()

	var cs []byte
	results, runErr := run(inner)

	// A statement ended our transaction behind our back: the session holds
	// changes the database no longer has.
	if raw.autocommit() {
		return nil, nil, ErrNotCapturable
	}
	if runErr != nil {
		return results, nil, runErr
	}
	if cs, err = sess.changeset(); err != nil {
		return nil, nil, err
	}
	if req.Transaction && hasError(results) {
		// The statement path rolls back the whole request on the first error.
		return results, nil, nil
	}

	var cookieAfter int64
	if err := c.conn.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&cookieAfter); err != nil {
		return nil, nil, classifyError(err)
	}
	if cookieAfter != cookieBefore {
		// DDL ran. The session does not record it.
		return nil, nil, ErrNotCapturable
	}
	return results, cs, nil
}

func hasError(results []*command.ExecuteQueryResponse) bool {
	for _, r := range results {
		if r.GetE().GetError() != "" || r.GetQ().GetError() != "" {
			return true
		}
	}
	return false
}

// ApplyChangeset applies a changeset recorded by ExecuteCapture. Any conflict
// aborts the apply and rolls it back entirely, so the outcome is the same on
// every node whose database matches the leader's.
func (c *Conn) ApplyChangeset(changeset []byte) error {
	return classifyError(c.conn.Raw(func(driverConn any) error {
		raw, err := rawHandles(driverConn)
		if err != nil {
			return err
		}
		return raw.applyChangeset(changeset)
	}))
}

// ApplyChangeset applies a changeset to the database.
func (db *DB) ApplyChangeset(ctx context.Context, changeset []byte) (retErr error) {
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return err
	}
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	stats.Add(numChangesetApplies, 1)
	return conn.ApplyChangeset(changeset)
}

// ExecuteCapture records the changeset for an execute request. See
// Conn.ExecuteCapture.
func (db *DB) ExecuteCapture(ctx context.Context, req *command.Request, xTime bool) (results []*command.ExecuteQueryResponse, changeset []byte, retErr error) {
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	return conn.ExecuteCapture(ctx, req, xTime)
}

// RequestCapture records the changeset for a unified request. See
// Conn.RequestCapture.
func (db *DB) RequestCapture(ctx context.Context, req *command.Request, xTime bool) (results []*command.ExecuteQueryResponse, changeset []byte, retErr error) {
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	return conn.RequestCapture(ctx, req, xTime)
}

// session is a SQLite session attached to every table of the main database.
type session struct {
	raw rawConn
	ptr uintptr
}

// newSession creates a session on the main database and attaches every table to
// it, including tables created later. Tables without an explicit PRIMARY KEY are
// recorded under their rowid.
func (r rawConn) newSession() (*session, error) {
	zMain, err := libc.CString("main")
	if err != nil {
		return nil, err
	}
	defer libc.Xfree(r.tls, zMain)

	bp := libc.Xmalloc(r.tls, ptrSize)
	if bp == 0 {
		return nil, r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	*(*uintptr)(cPtr(bp)) = 0
	if rc := sqlite3.Xsqlite3session_create(r.tls, r.db, zMain, bp); rc != sqlite3.SQLITE_OK {
		return nil, r.errstr(rc)
	}
	s := &session{raw: r, ptr: *(*uintptr)(cPtr(bp))}

	// Record tables that have no explicit PRIMARY KEY, using the rowid.
	*(*int32)(cPtr(bp)) = 1
	if rc := sqlite3.Xsqlite3session_object_config(r.tls, s.ptr, sqlite3.SQLITE_SESSION_OBJCONFIG_ROWID, bp); rc != sqlite3.SQLITE_OK {
		s.close()
		return nil, r.errstr(rc)
	}
	// A NULL table name attaches every table.
	if rc := sqlite3.Xsqlite3session_attach(r.tls, s.ptr, 0); rc != sqlite3.SQLITE_OK {
		s.close()
		return nil, r.errstr(rc)
	}
	return s, nil
}

func (s *session) close() {
	if s.ptr != 0 {
		sqlite3.Xsqlite3session_delete(s.raw.tls, s.ptr)
		s.ptr = 0
	}
}

// changeset returns the changeset recorded so far. It is nil if nothing changed.
func (s *session) changeset() ([]byte, error) {
	r := s.raw
	bp := libc.Xmalloc(r.tls, 2*ptrSize)
	if bp == 0 {
		return nil, r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	*(*int32)(cPtr(bp)) = 0
	*(*uintptr)(cPtr(bp + uintptr(ptrSize))) = 0
	rc := sqlite3.Xsqlite3session_changeset(r.tls, s.ptr, bp, bp+uintptr(ptrSize))
	if rc != sqlite3.SQLITE_OK {
		return nil, r.errstr(rc)
	}
	n := int(*(*int32)(cPtr(bp)))
	p := *(*uintptr)(cPtr(bp + uintptr(ptrSize)))
	if p == 0 || n == 0 {
		return nil, nil
	}
	defer sqlite3.Xsqlite3_free(r.tls, p)
	return append([]byte(nil), unsafeSlice(p, n)...), nil
}

// applyChangeset applies the changeset, aborting on the first conflict.
func (r rawConn) applyChangeset(changeset []byte) error {
	if len(changeset) == 0 {
		return nil
	}
	buf := libc.Xmalloc(r.tls, types.Size_t(len(changeset)))
	if buf == 0 {
		return r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, buf)
	copy(unsafeSlice(buf, len(changeset)), changeset)

	// The conflict handler records the kind of conflict here.
	conflict := libc.Xmalloc(r.tls, 4)
	if conflict == 0 {
		return r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, conflict)
	*(*int32)(cPtr(conflict)) = 0

	// The session recorded every row the request changed, including those
	// changed by triggers and by foreign key actions. Letting either fire again
	// during the apply would change those rows twice, so both are turned off:
	// triggers on the connection for the duration of the apply, foreign key
	// actions by the apply flag.
	if err := r.setTriggersEnabled(false); err != nil {
		return err
	}
	defer r.setTriggersEnabled(true)

	rc := sqlite3.Xsqlite3changeset_apply_v2(r.tls, r.db, int32(len(changeset)), buf, 0,
		cFuncPointer(abortOnConflict), conflict, 0, 0, sqlite3.SQLITE_CHANGESETAPPLY_FKNOACTION)
	if rc == sqlite3.SQLITE_OK {
		return nil
	}
	if kind := *(*int32)(cPtr(conflict)); kind != 0 {
		return fmt.Errorf("%w (kind %d, rc %d)", ErrChangesetConflict, kind, rc)
	}
	return r.errstr(rc)
}

// setTriggersEnabled turns trigger execution on the connection on or off.
func (r rawConn) setTriggersEnabled(on bool) error {
	const vaSlot = 8
	bp := libc.Xmalloc(r.tls, types.Size_t(2*vaSlot))
	if bp == 0 {
		return r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	v := int32(0)
	if on {
		v = 1
	}
	rc := sqlite3.Xsqlite3_db_config(r.tls, r.db, sqlite3.SQLITE_DBCONFIG_ENABLE_TRIGGER,
		libc.VaList(bp, v, uintptr(0)))
	if rc != sqlite3.SQLITE_OK {
		return r.errstr(rc)
	}
	return nil
}

// abortOnConflict is the conflict handler of every apply. It never resolves a
// conflict: replacing or skipping a row would let nodes diverge silently.
func abortOnConflict(_ *libc.TLS, pCtx uintptr, eConflict int32, _ uintptr) int32 {
	*(*int32)(cPtr(pCtx)) = eConflict
	return sqlite3.SQLITE_CHANGESET_ABORT
}

// unsafeSlice views n bytes of C memory at p as a slice.
func unsafeSlice(p uintptr, n int) []byte {
	return unsafe.Slice((*byte)(cPtr(p)), n)
}
