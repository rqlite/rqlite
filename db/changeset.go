package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode"
	"unsafe"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

// The C calls below are variables so that tests can make each one fail, which
// is the only way to reach the error paths that follow them.
var (
	cMalloc              = libc.Xmalloc
	cSessionCreate       = sqlite3.Xsqlite3session_create
	cSessionObjectConfig = sqlite3.Xsqlite3session_object_config
	cSessionAttach       = sqlite3.Xsqlite3session_attach
	cSessionChangeset    = sqlite3.Xsqlite3session_changeset
	cDBConfig            = sqlite3.Xsqlite3_db_config
	connHandles          = (*Conn).rawHandles
	cChangesetStart      = sqlite3.Xsqlite3changeset_start
)

// ErrNotCapturable is returned by ExecuteCapture and RequestCapture when the
// request cannot be replicated as a changeset. Nothing has been changed in the
// database, and the caller must run the request through the statement path.
var ErrNotCapturable = errors.New("request cannot be replicated as a changeset")

// DefaultChangesetMaxSize is the largest changeset, in bytes, a Leader will
// replicate when no other limit is set.
const DefaultChangesetMaxSize = 32 << 20

// ErrChangesetTooLarge is returned by ExecuteCapture and RequestCapture when the
// changeset would exceed the configured maximum. The request is refused: the
// statement path is not a fallback, because it would replicate the request's
// non-deterministic SQL, which is what changesets exist to avoid.
var ErrChangesetTooLarge = errors.New("changeset exceeds the maximum size")

// ErrChangesetWithCDC is returned when a capture is attempted on a connection
// that has a CDC preupdate hook. SQLite has one preupdate hook slot per
// connection, the session extension takes it, and chaining the two is not
// implemented.
var ErrChangesetWithCDC = errors.New("changeset replication cannot be combined with CDC")

// ErrChangesetMalformed is returned when bytes handed to ApplyChangeset are not a
// valid changeset. The refusal is the same on every node, and the node's
// database is not at fault, so it is not a FatalError.
var ErrChangesetMalformed = errors.New("malformed changeset")

// ErrChangesetSchema is returned when the changeset names a table this node
// does not have, or has with another shape. SQLite would skip such a table and
// report success.
var ErrChangesetSchema = errors.New("changeset does not match this node's schema")

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
			// An unterminated comment leaves nothing, which yields "".
			_, sql, _ = strings.Cut(sql, "\n")
		case strings.HasPrefix(sql, "/*"):
			_, sql, _ = strings.Cut(sql[2:], "*/")
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

// captureAllowed refuses a capture on a database that has a CDC hook.
func (db *DB) captureAllowed() error {
	if db.cfg != nil && db.cfg.Writer.PreUpdateHook != nil {
		return ErrChangesetWithCDC
	}
	return nil
}

// SetChangesetMaxSize sets the largest changeset the next captures may produce.
// Zero or less selects DefaultChangesetMaxSize.
func (db *DB) SetChangesetMaxSize(n int64) { db.changesetMax.Store(n) }

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

	// Raw holds the connection for the duration of its callback, so the
	// handles are taken out of it and used afterwards. This is safe because
	// the caller holds the only read-write connection.
	raw, err := connHandles(c)
	if err != nil {
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

	sess, err := raw.newSession()
	if err != nil {
		return nil, nil, err
	}
	defer sess.close()

	// The authorizer is removed before the session and the transaction are
	// closed (deferred calls run last in, first out), since it refuses ROLLBACK.
	auth := raw.installAuthorizer()
	removeAuth := sync.OnceFunc(raw.removeAuthorizer)
	defer removeAuth()

	// Statements run one at a time, and the moment the transaction is gone
	// nothing more runs: a statement after it would commit on this node alone.
	// The authorizer also refuses to prepare anything once the transaction has
	// ended, which covers the tail of a multi-statement string.
	var cs []byte
	var runErr error
	changesBefore := raw.totalChanges()
	touched, unhook := raw.countTouchedRows()
	defer unhook()
	for _, stmt := range inner.Statements {
		one := &command.Request{
			Statements:     []*command.Statement{stmt},
			DbTimeout:      inner.DbTimeout,
			QualifyColumns: inner.QualifyColumns,
		}
		r, err := run(one)
		results = append(results, r...)
		if auth.refused() {
			// Nothing is committed: the transaction, if it still exists, is
			// rolled back on return, and the caller takes the statement path.
			return nil, nil, ErrNotCapturable
		}
		if raw.autocommit() {
			// Nothing was denied, yet the transaction is gone: SQLite ended it.
			return nil, nil, transactionEnded(err, r)
		}
		if err != nil {
			runErr = err
			break
		}
		if req.Transaction && hasError(r) {
			// The statement path stops at the first error of a transaction.
			break
		}
		if !req.Transaction && !hasError(r) && raw.deferredFKs() > 0 {
			// On the statement path this statement commits on its own, and the
			// commit fails on the violation. Only the statement path reports that.
			return nil, nil, ErrNotCapturable
		}
	}
	removeAuth()
	if runErr != nil {
		return results, nil, runErr
	}
	if req.Transaction && hasError(results) {
		// The statement path rolls back the whole request on the first error.
		return results, nil, nil
	}
	if raw.deferredFKs() > 0 {
		// The commit the statement path makes at the end would fail.
		return nil, nil, ErrNotCapturable
	}

	// Cheap pre-check, before the changeset is built: the session records only
	// main-schema tables with a key it can address. It is a prediction, and the
	// count below is what decides.
	if ok, err := c.tablesRecordable(ctx, auth.writtenTables()); err != nil {
		return nil, nil, classifyError(err)
	} else if !ok {
		return nil, nil, ErrNotCapturable
	}

	max := c.changesetMax
	if max <= 0 {
		max = DefaultChangesetMaxSize
	}
	// The session knows how large the changeset will be before it is built.
	if size := sess.size(); size > max {
		return nil, nil, fmt.Errorf("%w: %d bytes, limit %d", ErrChangesetTooLarge, size, max)
	}
	if cs, err = sess.changeset(); err != nil {
		return nil, nil, err
	}

	// Verify, instead of predicting: the changeset must account for every row
	// the request changed. It does when it has one operation per row change
	// (this also covers WITHOUT ROWID tables, which fire no update hook), or one
	// per distinct row touched, since the session folds several changes of one
	// row into one operation. A row the session did not record (a NULL key, a
	// table it cannot see, a net-zero insert and delete, an update to the same
	// value) shows as fewer operations than either count, and a REPLACE that
	// deleted another row shows as more. Anything else is replicated as statements.
	ops, err := raw.changesetOps(cs)
	if err != nil {
		return nil, nil, err
	}
	if int64(ops) != raw.totalChanges()-changesBefore && ops != touched.count() {
		return nil, nil, ErrNotCapturable
	}

	// DDL cannot have run: the authorizer refuses it, and refusal is checked above.
	return results, cs, nil
}

// transactionEnded builds the error for a capture transaction that SQLite
// rolled back by itself. A timeout or an interrupt does that, and is an error
// of the request: the statement path would run the same statements for as long
// again. Anything else (INSERT OR ROLLBACK, RAISE(ROLLBACK)) is a deterministic
// request that only the statement path replicates.
func transactionEnded(err error, r []*command.ExecuteQueryResponse) error {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	for _, x := range r {
		if e := x.GetE().GetError(); e != "" {
			msg += " " + e
		}
		if e := x.GetQ().GetError(); e != "" {
			msg += " " + e
		}
	}
	for _, s := range []string{"interrupt", "timeout", "deadline exceeded", "canceled"} {
		if strings.Contains(msg, s) {
			return fmt.Errorf("changeset capture was interrupted and rolled back: %s", strings.TrimSpace(msg))
		}
	}
	return fmt.Errorf("%w: the transaction ended: %s", ErrNotCapturable, strings.TrimSpace(msg))
}

func hasError(results []*command.ExecuteQueryResponse) bool {
	for _, r := range results {
		if r.GetE().GetError() != "" || r.GetQ().GetError() != "" {
			return true
		}
	}
	return false
}

// checkChangesetLen refuses a changeset whose length, plus the slots allocated
// beside it, does not fit the 32-bit length SQLite takes.
func checkChangesetLen(n int) error {
	if n > math.MaxInt32-64 {
		return fmt.Errorf("%w: %d bytes", ErrChangesetTooLarge, n)
	}
	return nil
}

// ApplyChangeset applies a changeset recorded by ExecuteCapture. Any conflict
// aborts the apply and rolls it back entirely, so the outcome is the same on
// every node whose database matches the leader's.
func (c *Conn) ApplyChangeset(changeset []byte) error {
	raw, err := c.rawHandles()
	if err != nil {
		return classifyError(err)
	}
	if err := checkChangesetLen(len(changeset)); err != nil {
		return err
	}
	if len(changeset) == 0 {
		return nil
	}
	// Walk the whole changeset and compare its tables with ours before anything
	// is applied: SQLite skips a table it cannot match and still reports success.
	tables, err := raw.readChangeset(changeset)
	if err != nil {
		return err
	}
	if err := c.checkChangesetShape(context.Background(), tables); err != nil {
		return classifyError(err)
	}
	err = raw.applyChangeset(changeset)
	var se *SQLiteError
	if errors.As(err, &se) && se.Code == sqlite3.SQLITE_CORRUPT {
		// The session extension reports bytes it cannot parse as CORRUPT, the code
		// of a damaged database file, and the walk above does not see every such
		// case. Only the file can tell them apart: a damaged one is a failure of
		// this node and fatal, a sound one means the bytes are at fault.
		if verdict, qerr := quickCheck(c); qerr != nil || verdict != "ok" {
			return classifyError(err)
		}
		return fmt.Errorf("%w: %s", ErrChangesetMalformed, se.Error())
	}
	return classifyError(err)
}

// quickCheck is a variable so that a test can report a damaged file, which
// cannot otherwise be produced on a live connection.
var quickCheck = func(c *Conn) (verdict string, err error) {
	err = c.conn.QueryRowContext(context.Background(), "PRAGMA quick_check(1)").Scan(&verdict)
	return verdict, err
}

// rawHandles returns the raw SQLite handles behind the connection.
func (c *Conn) rawHandles() (raw rawConn, err error) {
	err = c.conn.Raw(func(driverConn any) error {
		var err error
		raw, err = rawHandles(driverConn)
		return err
	})
	return raw, err
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
	if err := db.captureAllowed(); err != nil {
		return nil, nil, err
	}
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	conn.changesetMax = db.changesetMax.Load()
	return conn.ExecuteCapture(ctx, req, xTime)
}

// RequestCapture records the changeset for a unified request. See
// Conn.RequestCapture.
func (db *DB) RequestCapture(ctx context.Context, req *command.Request, xTime bool) (results []*command.ExecuteQueryResponse, changeset []byte, retErr error) {
	if err := db.captureAllowed(); err != nil {
		return nil, nil, err
	}
	conn, err := db.AcquireWriter(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { preserveFatalError(&retErr, conn.Close()) }()
	conn.changesetMax = db.changesetMax.Load()
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
	// One buffer holds the output slot of the calls below and, after it, the
	// NUL-terminated database name.
	bp := cMalloc(r.tls, ptrSize+8)
	if bp == 0 {
		return nil, r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	zMain := bp + uintptr(ptrSize)
	copy(unsafeSlice(zMain, 5), "main\x00")
	*(*uintptr)(cPtr(bp)) = 0
	if rc := cSessionCreate(r.tls, r.db, zMain, bp); rc != sqlite3.SQLITE_OK {
		return nil, r.errstr(rc)
	}
	s := &session{raw: r, ptr: *(*uintptr)(cPtr(bp))}

	// Track the size of the changeset as it grows, so that an oversized one is
	// refused before its buffer is built. Tables without an explicit PRIMARY KEY
	// are not recorded at all (see tablesRecordable), so the rowid mode is off.
	*(*int32)(cPtr(bp)) = 1
	if rc := cSessionObjectConfig(r.tls, s.ptr, sqlite3.SQLITE_SESSION_OBJCONFIG_SIZE, bp); rc != sqlite3.SQLITE_OK {
		s.close()
		return nil, r.errstr(rc)
	}
	// A NULL table name attaches every table.
	if rc := cSessionAttach(r.tls, s.ptr, 0); rc != sqlite3.SQLITE_OK {
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

// size returns the size in bytes of the changeset recorded so far.
func (s *session) size() int64 {
	return sqlite3.Xsqlite3session_changeset_size(s.raw.tls, s.ptr)
}

// changeset returns the changeset recorded so far. It is nil if nothing changed.
func (s *session) changeset() ([]byte, error) {
	r := s.raw
	bp := cMalloc(r.tls, 2*ptrSize)
	if bp == 0 {
		return nil, r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, bp)
	*(*int32)(cPtr(bp)) = 0
	*(*uintptr)(cPtr(bp + uintptr(ptrSize))) = 0
	rc := cSessionChangeset(r.tls, s.ptr, bp, bp+uintptr(ptrSize))
	if rc != sqlite3.SQLITE_OK {
		return nil, r.errstr(rc)
	}
	n := int(*(*int32)(cPtr(bp)))
	p := *(*uintptr)(cPtr(bp + uintptr(ptrSize)))
	// With nothing recorded, n is 0 and the copy below is nil; freeing a NULL p
	// is a no-op.
	defer sqlite3.Xsqlite3_free(r.tls, p)
	return append([]byte(nil), unsafeSlice(p, n)...), nil
}

// applyChangeset applies the changeset, aborting on the first conflict.
func (r rawConn) applyChangeset(changeset []byte) (retErr error) {
	// One allocation holds the changeset, then the slot in which the conflict
	// handler records the kind of conflict, then the argument list of db_config.
	// Nothing is allocated after the triggers are turned off, so turning them
	// back on cannot fail for lack of memory.
	off := (len(changeset) + 7) &^ 7
	buf := cMalloc(r.tls, types.Size_t(off+24))
	if buf == 0 {
		return r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, buf)
	copy(unsafeSlice(buf, len(changeset)), changeset)
	conflict := buf + uintptr(off)
	va := conflict + 8
	*(*int32)(cPtr(conflict)) = 0

	// The session recorded every row the request changed, including those
	// changed by triggers and by foreign key actions. Letting either fire again
	// during the apply would change those rows twice, so both are turned off:
	// triggers on the connection for the duration of the apply, foreign key
	// actions by the apply flag.
	if err := r.setTriggersEnabled(false, va); err != nil {
		return err
	}
	defer func() {
		// A connection left with triggers off would skip them on every later
		// write, so a failure here is not recoverable.
		if err := r.setTriggersEnabled(true, va); err != nil {
			retErr = &FatalError{err: err}
		}
	}()

	rc := sqlite3.Xsqlite3changeset_apply_v2(r.tls, r.db, int32(len(changeset)), buf, 0,
		cFuncPointer(abortOnConflict), conflict, 0, 0, sqlite3.SQLITE_CHANGESETAPPLY_FKNOACTION)
	if rc == sqlite3.SQLITE_OK {
		return nil
	}
	if kind := *(*int32)(cPtr(conflict)); kind != 0 {
		return fmt.Errorf("%w (kind %d, rc %d)", ErrChangesetConflict, kind, rc)
	}
	// The whole changeset was walked before this call, so the bytes are known to
	// parse: a SQLITE_CORRUPT here is the database file, and is fatal like any
	// other node-local failure (the caller classifies it).
	return r.errstr(rc)
}

// setTriggersEnabled turns trigger execution on the connection on or off. va
// is 16 bytes of C memory for db_config's argument list.
func (r rawConn) setTriggersEnabled(on bool, va uintptr) error {
	v := int32(0)
	if on {
		v = 1
	}
	rc := cDBConfig(r.tls, r.db, sqlite3.SQLITE_DBCONFIG_ENABLE_TRIGGER,
		libc.VaList(va, v, uintptr(0)))
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
