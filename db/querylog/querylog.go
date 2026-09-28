package querylog

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// QueryLogger logs SQL statements and their durations using SQLite trace_v2.
// Safe for concurrent use.
type QueryLogger struct {
	logger        *log.Logger
	minDuration   time.Duration
	noExpandedSQL bool

	mu      sync.Mutex
	pending map[traceKey]string
}

// New creates a QueryLogger from cfg.
func New(cfg *Config) *QueryLogger {
	return newWithLogger(cfg, log.New(os.Stderr, "[db-query] ", log.LstdFlags))
}

// Close is a no-op placeholder reserved for when file-based log output
// is added. Callers should defer ql.Close() for forward compatibility.
func (ql *QueryLogger) Close() error {
	return nil
}

// TraceHook is the SQLite trace callback. It handles STMT and PROFILE
// events and ignores all others. It is safe for concurrent use.
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

		ql.logger.Printf("%s (%s)", sql, dur)
	}

	return 0
}

// newWithLogger creates a QueryLogger from cfg using the supplied logger.
// Passing a nil logger is a supported state: TraceHook becomes a no-op,
// which is useful in tests that only verify non-interference.
// This constructor is intentionally unexported so same-package tests can
// capture log output without adding a public test-only API.
func newWithLogger(cfg *Config, l *log.Logger) *QueryLogger {
	var minDur time.Duration
	var noExpanded bool
	if cfg != nil {
		if cfg.MinDuration != nil {
			minDur = *cfg.MinDuration
		}
		noExpanded = cfg.NoExpandedSQL
	}
	return &QueryLogger{
		logger:        l,
		minDuration:   minDur,
		noExpandedSQL: noExpanded,
		pending:       make(map[traceKey]string),
	}
}

// traceKey identifies an in-flight statement by connection and statement handle.
// Both handles are needed since statement handles can be reused across connections.
type traceKey struct {
	ConnHandle uintptr
	StmtHandle uintptr
}
