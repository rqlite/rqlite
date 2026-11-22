package db

import (
	"context"
	"fmt"
	"strings"

	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

// tableColumn is one column of a table, as PRAGMA table_xinfo reports it.
type tableColumn struct {
	name    string
	typ     string
	notNull bool
	pk      bool
}

// tableInfo returns the kind of the main-schema object called name ("table",
// "view", "virtual", ...), whether it is WITHOUT ROWID, and its columns as the
// session extension counts them (generated columns included). kind is "" if
// there is no such object.
func (c *Conn) tableInfo(ctx context.Context, name string) (kind string, withoutRowid bool, cols []tableColumn, err error) {
	var wr int
	if err := queryAll(ctx, c, `SELECT type, wr FROM pragma_table_list WHERE schema = 'main' AND name = ?`,
		[]any{name}, func() []any { return []any{&kind, &wr} }, func() error {
			withoutRowid = wr != 0
			return nil
		}); err != nil {
		return "", false, nil, err
	}

	var col tableColumn
	var nn, pk, hidden int
	if err := queryAll(ctx, c, `SELECT name, type, "notnull", pk, hidden FROM pragma_table_xinfo(?, 'main') ORDER BY cid`,
		[]any{name}, func() []any { return []any{&col.name, &col.typ, &nn, &pk, &hidden} }, func() error {
			if hidden != 0 { // Hidden and generated columns are not in a changeset.
				return nil
			}
			col.notNull, col.pk = nn != 0, pk > 0
			cols = append(cols, col)
			return nil
		}); err != nil {
		return "", false, nil, err
	}
	return kind, withoutRowid, cols, nil
}

// queryAll runs a query and calls each once per row, after scanning the row into
// the destinations dest returns. It is a variable so that tests can make any one
// of the queries fail, which is the only way to reach the error paths after them.
var queryAll = func(ctx context.Context, c *Conn, query string, args []any, dest func() []any, each func() error) error {
	rows, err := c.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(dest()...); err != nil {
			return err
		}
		if err := each(); err != nil {
			return err
		}
	}
	return rows.Err()
}

// tablesRecordable reports whether the session extension records every change
// the request can have made to the named tables, and to the tables a foreign key
// action can reach from them. It refuses:
//
//   - tables without an explicit PRIMARY KEY: they would be addressed by rowid,
//     which VACUUM renumbers on each node at its own time;
//   - rowid tables whose key is not an INTEGER PRIMARY KEY and has a nullable
//     column: SQLite accepts a NULL there, and the session does not record such
//     a row, so the write would be acknowledged and lost;
//   - virtual tables, which the session does not record.
//
// Views are skipped: a write to a view runs its INSTEAD OF triggers, whose
// statements the authorizer reports against their own tables.
func (c *Conn) tablesRecordable(ctx context.Context, written []string) (bool, error) {
	set := make(map[string]struct{}, len(written))
	for _, t := range written {
		set[strings.ToLower(t)] = struct{}{}
	}

	// Foreign key actions run without the authorizer being told about them, so
	// every table that references a written one is treated as written, and so on.
	type edge struct{ child, parent string }
	var edges []edge
	var e edge
	if err := queryAll(ctx, c,
		`SELECT m.name, f."table" FROM main.sqlite_master m, pragma_foreign_key_list(m.name, 'main') f WHERE m.type = 'table'`,
		nil, func() []any { return []any{&e.child, &e.parent} }, func() error {
			edges = append(edges, e)
			return nil
		}); err != nil {
		return false, err
	}
	names := append([]string(nil), written...)
	for grew := true; grew; {
		grew = false
		for _, e := range edges {
			_, hasParent := set[strings.ToLower(e.parent)]
			_, hasChild := set[strings.ToLower(e.child)]
			if hasParent && !hasChild {
				set[strings.ToLower(e.child)] = struct{}{}
				names = append(names, e.child)
				grew = true
			}
		}
	}

	for _, name := range names {
		kind, wr, cols, err := c.tableInfo(ctx, name)
		if err != nil {
			return false, err
		}
		switch kind {
		case "view":
			continue
		case "table":
		default:
			return false, nil
		}
		var pkCols []tableColumn
		for _, col := range cols {
			if col.pk {
				pkCols = append(pkCols, col)
			}
		}
		switch {
		case len(pkCols) == 0:
			return false, nil
		case wr:
			// A WITHOUT ROWID key column is NOT NULL by definition.
		case len(pkCols) == 1 && strings.EqualFold(pkCols[0].typ, "INTEGER"):
			// The rowid itself.
		default:
			for _, col := range pkCols {
				if !col.notNull {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// changesetTable is the shape of one table as a changeset names it.
type changesetTable struct {
	name string
	pk   []bool // One entry per column.
}

// readChangeset walks the whole changeset, which is how a malformed one is
// told from a valid one without applying anything, and returns the shape of
// every table it names.
func (r rawConn) readChangeset(cs []byte) ([]changesetTable, error) {
	tables, _, err := r.readChangesetOps(cs)
	return tables, err
}

// changesetOps returns the number of row operations in the changeset.
func (r rawConn) changesetOps(cs []byte) (int, error) {
	_, ops, err := r.readChangesetOps(cs)
	return ops, err
}

// malformedOrLocal turns a code the iterator returned into an error. Only
// SQLITE_CORRUPT can come from the bytes: the iterator reads memory and touches
// no database, so any other code (out of memory, I/O, full) is a failure of this
// node, which is fatal as it is on the statement path.
func (r rawConn) malformedOrLocal(rc int32) error {
	if rc&0xff == sqlite3.SQLITE_CORRUPT {
		return fmt.Errorf("%w (rc %d)", ErrChangesetMalformed, rc)
	}
	return classifyError(r.errstr(rc))
}

func (r rawConn) readChangesetOps(cs []byte) ([]changesetTable, int, error) {
	n := (len(cs) + 7) &^ 7
	buf := cMalloc(r.tls, types.Size_t(n+48))
	if buf == 0 {
		return nil, 0, classifyError(r.errstr(sqlite3.SQLITE_NOMEM))
	}
	defer libc.Xfree(r.tls, buf)
	copy(unsafeSlice(buf, len(cs)), cs)
	// Slots after the changeset: iterator, table name, column count, operation,
	// indirect flag, primary key flags, column count.
	sc := buf + uintptr(n)
	pIter, pTab, pCol, pOp, pInd, pPK, pCol2 := sc, sc+8, sc+16, sc+20, sc+24, sc+32, sc+40
	*(*uintptr)(cPtr(pIter)) = 0

	if rc := cChangesetStart(r.tls, pIter, int32(len(cs)), buf); rc != sqlite3.SQLITE_OK {
		return nil, 0, r.malformedOrLocal(rc)
	}
	iter := *(*uintptr)(cPtr(pIter))
	defer sqlite3.Xsqlite3changeset_finalize(r.tls, iter)

	var tables []changesetTable
	ops := 0
	seen := make(map[string]int)
	rc := sqlite3.Xsqlite3changeset_next(r.tls, iter)
	for ; rc == sqlite3.SQLITE_ROW; rc = sqlite3.Xsqlite3changeset_next(r.tls, iter) {
		// op and pk cannot fail while next has returned a row.
		sqlite3.Xsqlite3changeset_op(r.tls, iter, pTab, pCol, pOp, pInd)
		ops++
		name := libc.GoString(*(*uintptr)(cPtr(pTab)))
		nCol := int(*(*int32)(cPtr(pCol)))
		if i, ok := seen[name]; ok {
			if len(tables[i].pk) != nCol {
				return nil, 0, fmt.Errorf("%w: table %q has two column counts", ErrChangesetMalformed, name)
			}
			continue
		}
		sqlite3.Xsqlite3changeset_pk(r.tls, iter, pPK, pCol2)
		flags := *(*uintptr)(cPtr(pPK))
		pk := make([]bool, nCol)
		for i := range pk {
			pk[i] = *(*byte)(cPtr(flags + uintptr(i))) != 0
		}
		seen[name] = len(tables)
		tables = append(tables, changesetTable{name: name, pk: pk})
	}
	if rc != sqlite3.SQLITE_DONE {
		return nil, 0, r.malformedOrLocal(rc)
	}
	return tables, ops, nil
}

// checkChangesetShape fails if the node lacks a table the changeset names, or
// has it with another number of columns or another key. SQLite would skip such
// a table and report success, which would hide a diverged node.
func (c *Conn) checkChangesetShape(ctx context.Context, tables []changesetTable) error {
	for _, t := range tables {
		kind, _, cols, err := c.tableInfo(ctx, t.name)
		if err != nil {
			return err
		}
		if kind != "table" {
			return fmt.Errorf("%w: no table %q", ErrChangesetSchema, t.name)
		}
		if len(cols) != len(t.pk) {
			return fmt.Errorf("%w: table %q has %d columns, the changeset %d", ErrChangesetSchema, t.name, len(cols), len(t.pk))
		}
		for i, col := range cols {
			if col.pk != t.pk[i] {
				return fmt.Errorf("%w: table %q key differs at column %d", ErrChangesetSchema, t.name, i)
			}
		}
	}
	return nil
}
