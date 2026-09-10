package querylog

import (
	"log"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// traceKey identifies an in-flight statement by connection and statement handle.
// Both handles are needed since statement handles can be reused across connections.
type traceKey struct {
	ConnHandle uintptr
	StmtHandle uintptr
}

// QueryLogger logs SQL statements and their durations using SQLite trace_v2.
// Safe for concurrent use.
type QueryLogger struct {
	logger        *log.Logger
	minDuration   time.Duration
	noExpandedSQL bool

	mu      sync.Mutex
	pending map[traceKey]string
}

// NewQueryLogger creates a QueryLogger from cfg.
func NewQueryLogger(cfg Config) *QueryLogger {
	return &QueryLogger{
		logger:        cfg.Logger,
		minDuration:   cfg.MinDuration,
		noExpandedSQL: cfg.NoExpandedSQL,
		pending:       make(map[traceKey]string),
	}
}

// Close is a no-op. Reserved for when file output is added.
func (ql *QueryLogger) Close() error {
	return nil
}

// TraceHook is the SQLite trace callback. Handles STMT and PROFILE events;
// ignores all others.
func (ql *QueryLogger) TraceHook(info sqlite3.TraceInfo) int {
	if ql.logger == nil {
		return 0
	}

	switch info.EventCode {
	case sqlite3.TraceStmt:
		var sqlText string
		if !ql.noExpandedSQL && info.ExpandedSQL != "" {
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

		dur := time.Duration(info.RunTimeNanosec)
		if dur < ql.minDuration {
			return 0
		}

		ql.logger.Printf("%s [%s]", sql, dur)
	}

	return 0
}
