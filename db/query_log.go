package db

import (
	"log"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// QueryLogConfig is the configuration object passed to the DB to control
// query logging.
type QueryLogConfig struct {
	// Logger is the destination for query log lines. If nil, query logging is disabled.
	Logger *log.Logger
}

// traceKey uniquely identifies an in-flight statement execution.
type traceKey struct {
	ConnHandle uintptr
	StmtHandle uintptr
}

// QueryLogger receives SQLite trace_v2 events (STMT and PROFILE) and
// produces log lines containing the SQL statement and its execution duration.
type QueryLogger struct {
	config QueryLogConfig

	mu      sync.Mutex
	pending map[traceKey]string
}

// NewQueryLogger creates a new QueryLogger with the given configuration.
func NewQueryLogger(cfg QueryLogConfig) *QueryLogger {
	return &QueryLogger{
		config:  cfg,
		pending: make(map[traceKey]string),
	}
}

// TraceHook is the callback registered with SQLite via conn.SetTrace().
// It handles two event types:
//
//   - SQLITE_TRACE_STMT: fired at statement execution start. The SQL text
//     is buffered keyed by (ConnHandle, StmtHandle).
//
//   - SQLITE_TRACE_PROFILE: fired at statement execution end. The buffered
//     SQL is looked up and a log line "SQL [duration]" is emitted.
//
// All other event types are ignored.
func (ql *QueryLogger) TraceHook(info sqlite3.TraceInfo) int {
	if ql.config.Logger == nil {
		return 0
	}

	switch info.EventCode {
	case sqlite3.TraceStmt:
		sql := info.ExpandedSQL
		if sql == "" {
			sql = info.StmtOrTrigger
		}
		if sql == "" {
			return 0
		}

		key := traceKey{ConnHandle: info.ConnHandle, StmtHandle: info.StmtHandle}
		ql.mu.Lock()
		ql.pending[key] = sql
		ql.mu.Unlock()

	case sqlite3.TraceProfile:
		key := traceKey{ConnHandle: info.ConnHandle, StmtHandle: info.StmtHandle}
		ql.mu.Lock()
		sql, ok := ql.pending[key]
		if ok {
			delete(ql.pending, key)
		}
		ql.mu.Unlock()

		if !ok {
			ql.config.Logger.Printf("PROFILE event without preceding STMT (conn=0x%x, stmt=0x%x)",
				info.ConnHandle, info.StmtHandle)
			return 0
		}

		dur := time.Duration(info.RunTimeNanosec) * time.Nanosecond
		ql.config.Logger.Printf("%s [%s]", sql, dur)
	}

	return 0
}
