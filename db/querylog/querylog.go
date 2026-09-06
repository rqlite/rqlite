package querylog

import (
	"log"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// LoggerConfig holds the resolved runtime parameters used to construct a QueryLogger.
// It is the constructor argument for NewQueryLogger, and is also used directly by
// tests that need to build a logger without going through the flag/file path.
type LoggerConfig struct {
	// Logger is the destination for query log lines. If nil, query logging is disabled.
	Logger *log.Logger

	// MinDuration is the minimum execution time a statement must take before it is
	// logged. A value of 0 logs every traced statement regardless of duration.
	MinDuration time.Duration

	// ExpandedSQL controls which SQL text is used
	ExpandedSQL bool
}

// traceKey uniquely identifies an in-flight statement execution across connections.
// Using both handles is required because statement handles can be reused across
// different SQLite connections.
type traceKey struct {
	ConnHandle uintptr
	StmtHandle uintptr
}

// QueryLogger receives SQLite trace_v2 STMT and PROFILE events and emits log
// lines of the form "SQL [duration]".
// It is safe for concurrent use by multiple SQLite connections.
type QueryLogger struct {
	logger      *log.Logger
	minDuration time.Duration
	expandedSQL bool

	mu      sync.Mutex
	pending map[traceKey]string
}

// NewQueryLogger creates a new QueryLogger from cfg.
func NewQueryLogger(cfg LoggerConfig) *QueryLogger {
	return &QueryLogger{
		logger:      cfg.Logger,
		minDuration: cfg.MinDuration,
		expandedSQL: cfg.ExpandedSQL,
		pending:     make(map[traceKey]string),
	}
}

// Close releases any resources owned by the QueryLogger. It is a no-op today
// because the logger currently always writes to os.Stderr, which is not owned
// by this package. If file output is added later, Close will close the file.
// Close must not be called on a nil QueryLogger; the caller must guard with
// "if ql != nil { defer ql.Close() }".
func (ql *QueryLogger) Close() error {
	return nil
}

// TraceHook is the callback registered with SQLite via conn.SetTrace(). It
// handles two event types:
//
//   - SQLITE_TRACE_STMT: fired at statement execution start. The SQL text is
//     buffered keyed by (ConnHandle, StmtHandle).
//
//   - SQLITE_TRACE_PROFILE: fired at statement execution end. The pending entry
//     is always removed first (even when below the MinDuration threshold or when
//     the statement is missing), then the duration filter is applied, and a log
//     line is emitted only when the threshold is met.
//
// All other event types are ignored.
func (ql *QueryLogger) TraceHook(info sqlite3.TraceInfo) int {
	if ql.logger == nil {
		return 0
	}

	switch info.EventCode {
	case sqlite3.TraceStmt:
		// Select which SQL text to buffer based on the ExpandedSQL preference.
		var sqlText string
		if ql.expandedSQL && info.ExpandedSQL != "" {
			sqlText = info.ExpandedSQL
		} else {
			sqlText = info.StmtOrTrigger
		}
		if sqlText == "" {
			return 0
		}

		key := traceKey{ConnHandle: info.ConnHandle, StmtHandle: info.StmtHandle}
		ql.mu.Lock()
		ql.pending[key] = sqlText
		ql.mu.Unlock()

	case sqlite3.TraceProfile:
		key := traceKey{ConnHandle: info.ConnHandle, StmtHandle: info.StmtHandle}

		// Always remove the pending entry before applying the duration filter.
		// Failing to do so would leak map entries for filtered or orphaned statements.
		ql.mu.Lock()
		sql, ok := ql.pending[key]
		if ok {
			delete(ql.pending, key)
		}
		ql.mu.Unlock()

		if !ok {
			ql.logger.Printf("PROFILE event without preceding STMT (conn=0x%x, stmt=0x%x)",
				info.ConnHandle, info.StmtHandle)
			return 0
		}

		dur := time.Duration(info.RunTimeNanosec) * time.Nanosecond

		// Apply duration filter. Statements below the threshold are silently
		// dropped; the pending entry has already been cleaned up above.
		if dur < ql.minDuration {
			return 0
		}

		ql.logger.Printf("%s [%s]", sql, dur)
	}

	return 0
}
