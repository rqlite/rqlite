package db

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"modernc.org/libc"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// errstrTLS is the libc thread state used to look up sqlite3_errstr, which
// reads static tables only. errstrMu serializes its use.
var (
	errstrTLS = libc.NewTLS()
	errstrMu  sync.Mutex
)

// sqliteErrstr returns sqlite3_errstr for the given result code.
func sqliteErrstr(code int32) string {
	errstrMu.Lock()
	defer errstrMu.Unlock()
	return libc.GoString(sqlite3.Xsqlite3_errstr(errstrTLS, code))
}

// errMessage returns the message rqlite reports for err. The pure-Go driver
// formats a SQLite error as "<errstr>: <errmsg> (<code>)", with " (SQLITE_BUSY)"
// appended for busy errors. The cgo driver reported <errmsg> alone, and so
// does rqlite, so the messages in the HTTP API do not change with the driver.
// An error which wraps a driver error with more text is left alone.
func errMessage(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	var drvErr *sqlite.Error
	if !errors.As(err, &drvErr) || drvErr.Error() != s {
		return s
	}
	return stripDriverErrorDecoration(s, int32(drvErr.Code()))
}

func stripDriverErrorDecoration(s string, code int32) string {
	s = strings.TrimSuffix(s, " (SQLITE_BUSY)")
	s = strings.TrimSuffix(s, fmt.Sprintf(" (%d)", code))
	if errstr := sqliteErrstr(code); s != errstr {
		s = strings.TrimPrefix(s, errstr+": ")
	}
	return s
}

// SQLiteError is a representation of the SQLite-level detailed error. It is
// also an error in its own right, so code outside the driver (and tests) can
// raise SQLite-coded errors.
type SQLiteError struct {
	Code         int32
	ExtendedCode int32
	SystemErrno  int32

	msg string
}

// NewSQLiteError returns a SQLite error carrying the given codes.
func NewSQLiteError(code, extendedCode int32, msg string) *SQLiteError {
	if extendedCode == 0 {
		extendedCode = code
	}
	return &SQLiteError{Code: code, ExtendedCode: extendedCode, msg: msg}
}

// Error implements error.
func (e *SQLiteError) Error() string {
	if e.msg != "" {
		return e.msg
	}
	if s, ok := sqlite.ErrorCodeString[int(e.ExtendedCode)]; ok {
		return s
	}
	if s, ok := sqlite.ErrorCodeString[int(e.Code)]; ok {
		return s
	}
	return fmt.Sprintf("sqlite error %d", e.ExtendedCode)
}

// ReadOnlyError returns true if the error is a read-only error.
func (e *SQLiteError) ReadOnlyError() bool {
	return e.Code == sqlite3.SQLITE_READONLY
}

// NewSQLiteErrorFromError extracts a full structured SQLite error from an error,
// returning nil if the error is not a SQLite error.
func NewSQLiteErrorFromError(err error) *SQLiteError {
	var se *SQLiteError
	if errors.As(err, &se) {
		return se
	}
	var drvErr *sqlite.Error
	if errors.As(err, &drvErr) {
		// The driver enables extended result codes, so Code() is the
		// extended code; its low byte is the primary code.
		code := int32(drvErr.Code())
		return &SQLiteError{
			Code:         code & 0xff,
			ExtendedCode: code,
			msg:          stripDriverErrorDecoration(drvErr.Error(), code),
		}
	}
	return nil
}

// newErrorProto returns the structured form of err. The message is always
// set. The SQLite result codes are present only when err wraps a SQLite
// error; timeouts, parameter conversion failures, and other non-SQLite errors
// carry no codes.
func newErrorProto(err error) *command.Error {
	e := &command.Error{
		Message: errMessage(err),
	}
	if se := NewSQLiteErrorFromError(err); se != nil {
		e.Sqlite = &command.SQLiteErrorCodes{
			Code:         se.Code,
			ExtendedCode: se.ExtendedCode,
			SystemErrno:  se.SystemErrno,
		}
	}
	return e
}

// newExecuteResultError returns an ExecuteResult describing err, carrying
// both the legacy error message and the structured error.
func newExecuteResultError(err error) *command.ExecuteResult {
	return &command.ExecuteResult{
		Error:   errMessage(err),
		ErrorV2: newErrorProto(err),
	}
}

// newQueryRowsError returns a QueryRows describing err, carrying both the
// legacy error message and the structured error.
func newQueryRowsError(err error) *command.QueryRows {
	rows := &command.QueryRows{}
	setQueryRowsError(rows, err)
	return rows
}

// setQueryRowsError records err on an existing QueryRows, carrying both the
// legacy error message and the structured error.
func setQueryRowsError(rows *command.QueryRows, err error) {
	rows.Error = errMessage(err)
	rows.ErrorV2 = newErrorProto(err)
}

// FatalError indicates that a database operation failed for a reason which may
// differ between replicas. Callers applying replicated commands must stop normal
// operation and recover from a known-good state rather than skip the command.
// The DB package reports this condition but does not terminate the process.
type FatalError struct {
	err error
}

func (e *FatalError) Error() string { return e.err.Error() }

// Unwrap returns the original error, including any SQLite error details.
func (e *FatalError) Unwrap() error { return e.err }

// nodeLocalErrorCodes identifies errors that cannot safely be treated as
// deterministic SQL failures. Primary codes include all their extended variants.
// Busy and locked errors are fatal on write paths once they escape SQLite's
// busy handling. The read-only Query API retains ordinary error handling.
// Interrupts and context cancellation retain their existing, separate policy.
var nodeLocalErrorCodes = map[int32]struct{}{
	sqlite3.SQLITE_FULL:     {}, // Storage capacity exhausted.
	sqlite3.SQLITE_IOERR:    {}, // Filesystem or device failure.
	sqlite3.SQLITE_NOMEM:    {}, // Memory allocation failure.
	sqlite3.SQLITE_CORRUPT:  {}, // Database corruption.
	sqlite3.SQLITE_NOTADB:   {}, // Invalid database file.
	sqlite3.SQLITE_CANTOPEN: {}, // Database or temporary file cannot be opened.
	sqlite3.SQLITE_PERM:     {}, // Filesystem access denied.
	sqlite3.SQLITE_READONLY: {}, // Required write access unavailable.
	sqlite3.SQLITE_NOLFS:    {}, // Required filesystem capability unavailable.
	sqlite3.SQLITE_PROTOCOL: {}, // Locking protocol failure.
	sqlite3.SQLITE_BUSY:     {}, // Contention with another connection.
	sqlite3.SQLITE_LOCKED:   {}, // Connection or shared-cache contention.
	sqlite3.SQLITE_INTERNAL: {}, // Engine malfunction.
	sqlite3.SQLITE_MISUSE:   {}, // Invalid engine usage.
}

// classifyError preserves ordinary errors and wraps node-local SQLite failures.
func classifyError(err error) error {
	if err == nil || isFatalError(err) {
		return err
	}
	if se := NewSQLiteErrorFromError(err); se != nil {
		if _, ok := nodeLocalErrorCodes[se.Code]; ok {
			return &FatalError{err: err}
		}
	}
	return err
}

func isFatalError(err error) bool {
	var fatal *FatalError
	return errors.As(err, &fatal)
}

// preserveFatalError promotes fatal cleanup failures without replacing an
// earlier fatal error. Nonfatal cleanup errors retain their existing behavior.
func preserveFatalError(err *error, cleanupErr error) {
	*err = classifyError(*err)
	if cleanupErr = classifyError(cleanupErr); isFatalError(cleanupErr) && !isFatalError(*err) {
		*err = cleanupErr
	}
}
