package sqlite3

import (
	"sync"
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const (
	TraceStmt    = lib.SQLITE_TRACE_STMT
	TraceProfile = lib.SQLITE_TRACE_PROFILE
	TraceRow     = lib.SQLITE_TRACE_ROW
	TraceClose   = lib.SQLITE_TRACE_CLOSE
)

type TraceInfo struct {
	EventCode                  uint32
	ConnHandle, StmtHandle     uintptr
	StmtOrTrigger, ExpandedSQL string
	RunTimeNanosec             int64
}
type TraceConfig struct {
	Callback        func(TraceInfo) int
	EventMask       uint32
	WantExpandedSQL bool
}
type hooks struct {
	trace  *TraceConfig
	update func(int, string, string, int64)
}

var hookMap sync.Map

func (c *SQLiteConn) hooks() *hooks { h, _ := hookMap.LoadOrStore(c.db, &hooks{}); return h.(*hooks) }
func pointer[T any](f T) uintptr    { return *(*uintptr)(unsafe.Pointer(&f)) }
func (c *SQLiteConn) RegisterUpdateHook(f func(int, string, string, int64)) {
	c.hooks().update = f
	lib.Xsqlite3_update_hook(c.tls, c.db, pointer(updateHook), c.db)
}
func updateHook(t *libc.TLS, id uintptr, op int32, db, tbl uintptr, row int64) {
	if h, ok := hookMap.Load(id); ok {
		if f := h.(*hooks).update; f != nil {
			f(int(op), libc.GoString(db), libc.GoString(tbl), row)
		}
	}
}
func (c *SQLiteConn) SetTrace(cfg *TraceConfig) error {
	c.hooks().trace = cfg
	return c.rc(lib.Xsqlite3_trace_v2(c.tls, c.db, cfg.EventMask, pointer(traceHook), c.db))
}
func traceHook(t *libc.TLS, event uint32, id, p, x uintptr) int32 {
	h, ok := hookMap.Load(id)
	if !ok {
		return 0
	}
	cfg := h.(*hooks).trace
	if cfg == nil {
		return 0
	}
	info := TraceInfo{EventCode: event, ConnHandle: id, StmtHandle: p}
	switch event {
	case TraceStmt:
		info.StmtOrTrigger = libc.GoString(x)
		if cfg.WantExpandedSQL {
			z := lib.Xsqlite3_expanded_sql(t, p)
			info.ExpandedSQL = libc.GoString(z)
			lib.Xsqlite3_free(t, z)
		}
	case TraceProfile:
		info.RunTimeNanosec = *(*int64)(unsafe.Pointer(x))
	}
	return int32(cfg.Callback(info))
}
func (c *SQLiteConn) Close() error { defer hookMap.Delete(c.db); return c.Conn.Close() }
