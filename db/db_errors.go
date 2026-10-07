package db

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

// SQLiteError is a representation of the SQLite-level detailed error.
type SQLiteError struct {
	Code         int32
	ExtendedCode int32
	SystemErrno  int32
}

// ReadOnlyError returns true if the error is a read-only error.
func (e *SQLiteError) ReadOnlyError() bool {
	return e.Code == int32(sqlite3.ErrReadonly)
}

// NewSQLiteErrorFromError extracts a full structured SQLite error from an error,
// returning nil if the error is not a SQLite error.
func NewSQLiteErrorFromError(err error) *SQLiteError {
	var sqErr sqlite3.Error
	if errors.As(err, &sqErr) {
		return &SQLiteError{
			Code:         int32(sqErr.Code),
			ExtendedCode: int32(sqErr.ExtendedCode),
			SystemErrno:  int32(sqErr.SystemErrno),
		}
	}
	return nil
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
var nodeLocalErrorCodes = map[sqlite3.ErrNo]struct{}{
	sqlite3.ErrFull:     {}, // Storage capacity exhausted.
	sqlite3.ErrIoErr:    {}, // Filesystem or device failure.
	sqlite3.ErrNomem:    {}, // Memory allocation failure.
	sqlite3.ErrCorrupt:  {}, // Database corruption.
	sqlite3.ErrNotADB:   {}, // Invalid database file.
	sqlite3.ErrCantOpen: {}, // Database or temporary file cannot be opened.
	sqlite3.ErrPerm:     {}, // Filesystem access denied.
	sqlite3.ErrReadonly: {}, // Required write access unavailable.
	sqlite3.ErrNoLFS:    {}, // Required filesystem capability unavailable.
	sqlite3.ErrProtocol: {}, // Locking protocol failure.
	sqlite3.ErrBusy:     {}, // Contention with another connection.
	sqlite3.ErrLocked:   {}, // Connection or shared-cache contention.
	sqlite3.ErrInternal: {}, // Engine malfunction.
	sqlite3.ErrMisuse:   {}, // Invalid engine usage.
}

// classifyError preserves ordinary errors and wraps node-local SQLite failures.
func classifyError(err error) error {
	if err == nil || isFatalError(err) {
		return err
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		if _, ok := nodeLocalErrorCodes[sqliteErr.Code]; ok {
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
