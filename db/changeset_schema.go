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
	rows, err := c.conn.QueryContext(ctx, `SELECT type, wr FROM pragma_table_list WHERE schema = 'main' AND name = ?`, name)
	if err != nil {
		return "", false, nil, err
	}
	for rows.Next() {
		var wr int
		if err := rows.Scan(&kind, &wr); err != nil {
			rows.Close()
			return "", false, nil, err
		}
		withoutRowid = wr != 0
	}
	if err := rows.Close(); err != nil {
		return "", false, nil, err
	}
	if kind == "" {
		return "", false, nil, nil
	}

	rows, err = c.conn.QueryContext(ctx, `SELECT name, type, "notnull", pk, hidden FROM pragma_table_xinfo(?) ORDER BY cid`, name)
	if err != nil {
		return "", false, nil, err
	}
	for rows.Next() {
		var col tableColumn
		var nn, pk, hidden int
		if err := rows.Scan(&col.name, &col.typ, &nn, &pk, &hidden); err != nil {
			rows.Close()
			return "", false, nil, err
		}
		if hidden != 0 { // Hidden and generated columns are not in a changeset.
			continue
		}
		col.notNull, col.pk = nn != 0, pk > 0
		cols = append(cols, col)
	}
	return kind, withoutRowid, cols, rows.Close()
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
	if len(written) == 0 {
		return true, nil
	}
	set := make(map[string]struct{}, len(written))
	for _, t := range written {
		set[strings.ToLower(t)] = struct{}{}
	}

	// Foreign key actions run without the authorizer being told about them, so
	// every table that references a written one is treated as written, and so on.
	rows, err := c.conn.QueryContext(ctx,
		`SELECT m.name, f."table" FROM sqlite_master m, pragma_foreign_key_list(m.name) f WHERE m.type = 'table'`)
	if err != nil {
		return false, err
	}
	type edge struct{ child, parent string }
	var edges []edge
	for rows.Next() {
		var e edge
		if err := rows.Scan(&e.child, &e.parent); err != nil {
			rows.Close()
			return false, err
		}
		edges = append(edges, e)
	}
	if err := rows.Close(); err != nil {
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
	n := (len(cs) + 7) &^ 7
	buf := cMalloc(r.tls, types.Size_t(n+48))
	if buf == 0 {
		return nil, r.errstr(sqlite3.SQLITE_NOMEM)
	}
	defer libc.Xfree(r.tls, buf)
	copy(unsafeSlice(buf, len(cs)), cs)
	// Slots after the changeset: iterator, table name, column count, operation,
	// indirect flag, primary key flags, column count.
	sc := buf + uintptr(n)
	pIter, pTab, pCol, pOp, pInd, pPK, pCol2 := sc, sc+8, sc+16, sc+20, sc+24, sc+32, sc+40
	*(*uintptr)(cPtr(pIter)) = 0

	if rc := sqlite3.Xsqlite3changeset_start(r.tls, pIter, int32(len(cs)), buf); rc != sqlite3.SQLITE_OK {
		return nil, fmt.Errorf("%w (rc %d)", ErrChangesetMalformed, rc)
	}
	iter := *(*uintptr)(cPtr(pIter))
	defer sqlite3.Xsqlite3changeset_finalize(r.tls, iter)

	var tables []changesetTable
	seen := make(map[string]int)
	for {
		rc := sqlite3.Xsqlite3changeset_next(r.tls, iter)
		if rc == sqlite3.SQLITE_DONE {
			return tables, nil
		}
		if rc != sqlite3.SQLITE_ROW {
			return nil, fmt.Errorf("%w (rc %d)", ErrChangesetMalformed, rc)
		}
		if rc := sqlite3.Xsqlite3changeset_op(r.tls, iter, pTab, pCol, pOp, pInd); rc != sqlite3.SQLITE_OK {
			return nil, fmt.Errorf("%w (rc %d)", ErrChangesetMalformed, rc)
		}
		name := libc.GoString(*(*uintptr)(cPtr(pTab)))
		nCol := int(*(*int32)(cPtr(pCol)))
		if i, ok := seen[name]; ok {
			if len(tables[i].pk) != nCol {
				return nil, fmt.Errorf("%w: table %q has two column counts", ErrChangesetMalformed, name)
			}
			continue
		}
		if rc := sqlite3.Xsqlite3changeset_pk(r.tls, iter, pPK, pCol2); rc != sqlite3.SQLITE_OK {
			return nil, fmt.Errorf("%w (rc %d)", ErrChangesetMalformed, rc)
		}
		flags := *(*uintptr)(cPtr(pPK))
		pk := make([]bool, nCol)
		for i := range pk {
			pk[i] = *(*byte)(cPtr(flags + uintptr(i))) != 0
		}
		seen[name] = len(tables)
		tables = append(tables, changesetTable{name: name, pk: pk})
	}
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
