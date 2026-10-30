package db

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/progress"
	"modernc.org/sqlite"
)

// Conn is one connection to the database, and the unit of work: every
// statement runs on a connection, and a connection runs one thing at a time.
// Whether a connection can write is fixed when it is opened. Close returns
// the connection to the pool it was borrowed from.
type Conn struct {
	conn   *sql.Conn
	logger *log.Logger
}

func newConn(conn *sql.Conn, logger *log.Logger) *Conn {
	return &Conn{conn: conn, logger: logger}
}

// Close returns the connection to its pool.
func (c *Conn) Close() error {
	return c.conn.Close()
}

type execerQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Execute runs statements that modify the database.
// The earlier of the context's deadline and the request's timeout bounds
// the entire request.
func (c *Conn) Execute(ctx context.Context, req *command.Request, xTime bool) (allResults []*command.ExecuteQueryResponse, retErr error) {
	defer func() { retErr = classifyError(retErr) }()

	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}

	eqer := execerQueryer(c.conn)
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numETx, 1)
		var err error
		tx, err = c.conn.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer func() {
			if tx != nil {
				preserveFatalError(&retErr, tx.Rollback())
			}
		}()
		eqer = tx
	}

	// handleError sets the error field on the given result. It returns
	// whether the caller should continue processing or break.
	handleError := func(result *command.ExecuteQueryResponse, err error) bool {
		err = classifyError(err)
		if isFatalError(err) {
			retErr = err
		}
		stats.Add(numExecutionErrors, 1)
		if result == nil {
			result = &command.ExecuteQueryResponse{}
		}
		result.Result = &command.ExecuteQueryResponse_E{
			E: newExecuteResultError(err),
		}
		allResults = append(allResults, result)
		if tx != nil {
			preserveFatalError(&retErr, tx.Rollback())
			tx = nil
			return false
		}
		if req.RollbackOnError {
			// Use a background context here since the original context may have been canceled or hit its deadline,
			// and we want to ensure the rollback goes through.
			_, rollbackErr := c.executeStmt(context.Background(), &command.Statement{Sql: "ROLLBACK"}, false, eqer,
				time.Duration(req.DbTimeout))
			preserveFatalError(&retErr, rollbackErr)
			return false
		}
		return retErr == nil
	}

	// Execute each statement.
	for _, stmt := range req.Statements {
		ss := stmt.Sql
		if ss == "" {
			continue
		}

		result, err := c.executeStmt(ctx, stmt, xTime, eqer, time.Duration(req.DbTimeout))
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
		}
		allResults = append(allResults, result)
	}

	if tx != nil {
		retErr = tx.Commit()
	}
	return allResults, retErr
}

func (c *Conn) executeStmt(ctx context.Context, stmt *command.Statement, xTime bool, eq execerQueryer, timeout time.Duration) (res *command.ExecuteQueryResponse, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = classifyError(rewriteContextTimeout(retErr, ErrExecuteTimeout))
			if res != nil {
				res.Result = &command.ExecuteQueryResponse_E{
					E: newExecuteResultError(retErr),
				}
			}
		} else if res != nil && res.GetE().GetError() == "" && res.GetQ().GetError() == "" {
			res.Mutated = true
		}
	}()
	response := &command.ExecuteQueryResponse{}
	start := time.Now()

	parameters, err := parametersToValues(stmt.Parameters)
	if err != nil {
		response.Result = &command.ExecuteQueryResponse_E{
			E: newExecuteResultError(err),
		}
		return response, err
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// Reject an expired request before passing a later statement to SQLite,
	// which may finish a fast write before observing context cancellation.
	if err := ctx.Err(); err != nil {
		return response, err
	}

	if stmt.ForceQuery {
		stats.Add(numExecutionsForceQueries, 1)
		rows, err := c.queryStmt(ctx, stmt, xTime, eq)
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_Q{
				Q: newQueryRowsError(err),
			}
			return response, err
		}
		response.Result = &command.ExecuteQueryResponse_Q{
			Q: rows,
		}
	} else {
		result, err := eq.ExecContext(ctx, stmt.Sql, parameters...)
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_E{
				E: newExecuteResultError(err),
			}
			return response, err
		}
		if ctx.Err() != nil {
			return response, ctx.Err()
		}
		if result == nil {
			return response, nil
		}

		lid, ra, err := resultCounts(result)
		if err != nil {
			response.Result = &command.ExecuteQueryResponse_E{
				E: newExecuteResultError(err),
			}
			return response, err
		}
		tf := float64(0)
		if xTime {
			tf = time.Since(start).Seconds()
		}

		response.Result = &command.ExecuteQueryResponse_E{
			E: &command.ExecuteResult{
				LastInsertId: lid,
				RowsAffected: ra,
				Time:         tf,
			},
		}
	}
	return response, nil
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Query runs statements that return rows but do not modify the database.
// The earlier of the context's deadline and the request's timeout bounds
// the entire request.
func (c *Conn) Query(ctx context.Context, req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}

	var err error

	queryer := queryer(c.conn)
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numQTx, 1)
		tx, err = c.conn.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback() // Will be ignored if tx is committed
		queryer = tx
	}

	var allRows []*command.QueryRows
	for _, stmt := range req.Statements {
		sql := stmt.Sql
		if sql == "" {
			continue
		}

		var rows *command.QueryRows
		var err error

		rows, err = c.queryStmt(ctx, stmt, xTime, queryer)
		if err != nil {
			rows = newQueryRowsError(err)
			// Remap the message if necessary for backwards compatibility
			// reasons. The structured error retains the SQLite codes.
			se := NewSQLiteErrorFromError(err)
			if se != nil && se.ReadOnlyError() {
				rows.Error = ErrQueryWrite.Error()
			}
		}
		if req.QualifyColumns && rows != nil && rows.Error == "" {
			if qErr := c.qualifyColumns(stmt.Sql, rows); qErr != nil {
				c.logger.Printf("qualify columns: %s", qErr.Error())
			}
		}
		allRows = append(allRows, rows)
	}

	if tx != nil {
		err = tx.Commit()
	}
	return allRows, err
}

func (c *Conn) queryStmt(ctx context.Context, stmt *command.Statement, xTime bool, q queryer) (retRows *command.QueryRows, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = classifyError(rewriteContextTimeout(retErr, ErrQueryTimeout))
			if retRows != nil {
				setQueryRowsError(retRows, retErr)
			}
		}
	}()
	rows := &command.QueryRows{}
	start := time.Now()
	forceStall := stmt.ForceStall

	parameters, err := parametersToValues(stmt.Parameters)
	if err != nil {
		stats.Add(numQueryErrors, 1)
		setQueryRowsError(rows, err)
		return rows, nil
	}

	rs, err := q.QueryContext(ctx, stmt.Sql, parameters...)
	if err != nil {
		stats.Add(numQueryErrors, 1)
		setQueryRowsError(rows, err)
		return rows, err
	}
	defer func() { preserveFatalError(&retErr, rs.Close()) }()

	columns, err := rs.Columns()
	if err != nil {
		return nil, err
	}

	types, err := rs.ColumnTypes()
	if err != nil {
		return nil, err
	}
	xTypes := make([]string, len(types))
	for i := range types {
		xTypes[i] = strings.ToLower(types[i].DatabaseTypeName())
	}
	needsQueryTypes := containsEmptyType(xTypes)

	dest := make([]any, len(columns))
	ptrs := make([]any, len(dest))
	for rs.Next() && (len(columns) > 0 || forceStall) {
		for i := range ptrs {
			ptrs[i] = &dest[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		params, err := normalizeRowParameters(dest, xTypes)
		if err != nil {
			return nil, err
		}
		rows.Values = append(rows.Values, &command.Values{
			Parameters: params,
		})

		// Check for slow query, blocked query, etc. testing. This field
		// should never be set by production code and is only for fault-injection
		// testing purposes.
		if forceStall {
			select {
			case <-make(chan struct{}):
			case <-ctx.Done():
				c.logger.Printf("forced stall on query cancelled: %s", ctx.Err().Error())
				forceStall = false
			}
		}

		// One-time population of any empty types, using the values in the
		// first row.
		if needsQueryTypes {
			if err := populateEmptyTypes(xTypes, params); err != nil {
				return nil, err
			}
			needsQueryTypes = false
		}
	}

	// Check for errors from iterating over rows. The pure-Go driver does not
	// check the context inside Next, so an expired context is checked here
	// to report a timeout the way the cgo driver did.
	if err := rs.Err(); err != nil {
		stats.Add(numQueryErrors, 1)
		return rows, err
	}
	if err := ctx.Err(); err != nil {
		stats.Add(numQueryErrors, 1)
		return rows, err
	}

	if xTime {
		rows.Time = time.Since(start).Seconds()
	}

	rows.Columns = columns
	rows.Types = xTypes
	return rows, nil
}

// Request runs statements that may be a mix of executes and queries. Each
// statement is classified on this connection before it runs, so queries in a
// transaction see the writes that came before them.
// The earlier of the context's deadline and the request's timeout bounds
// the entire request.
func (c *Conn) Request(ctx context.Context, req *command.Request, xTime bool) (eqResponse []*command.ExecuteQueryResponse, retErr error) {
	defer func() { retErr = classifyError(retErr) }()

	if req.DbTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.DbTimeout))
		defer cancel()
	}

	var err error
	eq := execerQueryer(c.conn)
	var tx *sql.Tx
	if req.Transaction {
		stats.Add(numRTx, 1)
		tx, err = c.conn.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer func() {
			if tx != nil {
				preserveFatalError(&retErr, tx.Rollback())
			}
		}()
		eq = tx
	}

	// abortOnError indicates whether the caller should continue
	// processing or break.
	abortOnError := func(err error) bool {
		err = classifyError(err)
		if isFatalError(err) {
			retErr = err
		}
		if err != nil && tx != nil {
			preserveFatalError(&retErr, tx.Rollback())
			tx = nil
			return true
		}
		return retErr != nil
	}

	for _, stmt := range req.Statements {
		ss := stmt.Sql
		if ss == "" {
			continue
		}

		ro, err := c.StmtReadOnly(ss)
		if err != nil {
			eqResponse = append(eqResponse, &command.ExecuteQueryResponse{
				Result: &command.ExecuteQueryResponse_E{
					E: newExecuteResultError(err),
				},
			})
			if abortOnError(err) {
				break
			}
			continue
		}

		if ro {
			rows, opErr := c.queryStmt(ctx, stmt, xTime, eq)
			if req.QualifyColumns && rows != nil && rows.Error == "" {
				if qErr := c.qualifyColumns(stmt.Sql, rows); qErr != nil {
					if qErr = classifyError(qErr); isFatalError(qErr) {
						return eqResponse, qErr
					}
					c.logger.Printf("qualify columns: %s", qErr.Error())
				}
			}
			eqResponse = append(eqResponse, createEQQueryResponse(rows, opErr))
			if abortOnError(opErr) {
				break
			}
		} else {
			result, opErr := c.executeStmt(ctx, stmt, xTime, eq, time.Duration(req.DbTimeout))
			eqResponse = append(eqResponse, result)
			if abortOnError(opErr) {
				break
			}
		}
	}

	if tx != nil {
		retErr = tx.Commit()
	}
	return eqResponse, retErr
}

// Dump writes a consistent snapshot of the database in SQL text format.
// This function can be called when changes to the database are in flight.
// It returns the number of bytes written to w, even when it returns an error.
//
// When table names are given, the dump carries those tables and their data, plus
// the indexes and triggers that belong to them. Views are written out as they
// are, filtered or not, so a view that reads from a table the dump does not
// carry is created but cannot be queried. The dump itself still loads, since
// SQLite resolves the tables a view reads from when the view is used, not when it
// is created.
func (c *Conn) Dump(w io.Writer, tableNames ...string) (n int, retErr error) {
	cw := progress.NewCountingWriter(w)
	defer func() { n = int(cw.Count()) }()
	ctx := context.Background()

	// Keep schema and data reads on one snapshot, even if writes commit while
	// the dump is being streamed. End the read transaction on every exit path.
	// Because we need the transaction to span multiple queries we manually
	// manage the transaction.
	if _, err := c.conn.ExecContext(ctx, "BEGIN"); err != nil {
		return 0, err
	}
	defer func() {
		if _, err := c.conn.ExecContext(ctx, "ROLLBACK"); retErr == nil {
			retErr = err
		}
	}()

	// Convenience function to convert string query to protobuf.
	commReq := func(query string) *command.Request {
		return &command.Request{
			Statements: []*command.Statement{
				{
					Sql: query,
				},
			},
		}
	}

	if _, err := cw.Write([]byte("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")); err != nil {
		return 0, err
	}

	// Get the schema.
	rows, err := c.Query(ctx, DumpTablesReq(tableNames...), false)
	if err != nil {
		return 0, err
	}
	row := rows[0]
	for _, v := range row.Values {
		table := v.Parameters[0].GetS()

		var stmt string

		if table == "sqlite_sequence" {
			stmt = `DELETE FROM "sqlite_sequence";`
		} else if table == "sqlite_stat1" {
			stmt = `ANALYZE "sqlite_master";`
		} else if strings.HasPrefix(table, "sqlite_") {
			continue
		} else {
			stmt = v.Parameters[2].GetS()
		}

		if _, err := cw.Write(fmt.Appendf(nil, "%s;\n", stmt)); err != nil {
			return 0, err
		}

		tableIndent := strings.Replace(table, `"`, `""`, -1)
		r, err := c.Query(ctx, commReq(fmt.Sprintf(`PRAGMA table_info("%s")`, tableIndent)), false)
		if err != nil {
			return 0, err
		}
		var columnNames []string
		for _, vv := range r[0].Values {
			columnNames = append(columnNames, fmt.Sprintf(`'||quote("%s")||'`, vv.Parameters[1].GetS()))
		}

		query := fmt.Sprintf(`SELECT 'INSERT INTO "%s" VALUES(%s)' FROM "%s";`,
			tableIndent,
			strings.Join(columnNames, ","),
			tableIndent)
		r, err = c.Query(ctx, commReq(query), false)

		if err != nil {
			return 0, err
		}
		for _, x := range r[0].Values {
			y := fmt.Sprintf("%s;\n", x.Parameters[0].GetS())
			if _, err := cw.Write([]byte(y)); err != nil {
				return 0, err
			}
		}
	}

	// Do indexes, triggers, and views.
	objs, err := c.schemaObjects(ctx)
	if err != nil {
		return 0, err
	}
	for _, o := range objs.Filter(tableNames) {
		if _, err := cw.Write(fmt.Appendf(nil, "%s;\n", o.sql)); err != nil {
			return 0, err
		}
	}

	_, err = cw.Write([]byte("COMMIT;\n"))
	return 0, err
}

// StmtReadOnly returns whether the given SQL statement is read-only.
// As per https://www.sqlite.org/c3ref/stmt_readonly.html, this function
// may not return 100% correct results, but should cover most scenarios.
func (c *Conn) StmtReadOnly(sql string) (bool, error) {
	var readOnly bool
	f := func(driverConn any) error {
		raw, err := rawHandles(driverConn)
		if err != nil {
			return err
		}
		readOnly, err = raw.stmtReadOnly(sql)
		return err
	}

	if err := c.conn.Raw(f); err != nil {
		return false, err
	}
	return readOnly, nil
}

// schemaObjects returns every index, trigger and view of the database, in
// the order they were created.
func (c *Conn) schemaObjects(ctx context.Context) (schemaObjects, error) {
	req := &command.Request{
		Statements: []*command.Statement{
			{
				Sql: `SELECT "name", "type", "tbl_name", "sql" FROM "sqlite_master"
					  WHERE "sql" NOT NULL AND "type" IN ('index', 'trigger', 'view')
					  ORDER BY "rowid"`,
			},
		},
	}

	rows, err := c.Query(ctx, req, false)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if rows[0].Error != "" {
		return nil, fmt.Errorf("failed to read schema objects: %s", rows[0].Error)
	}

	objs := make(schemaObjects, 0, len(rows[0].Values))
	for _, v := range rows[0].Values {
		objs = append(objs, schemaObject{
			name:    v.Parameters[0].GetS(),
			typ:     v.Parameters[1].GetS(),
			tblName: v.Parameters[2].GetS(),
			sql:     v.Parameters[3].GetS(),
		})
	}
	return objs, nil
}

// qualifyColumns prefixes each column name in rows with its originating table name.
func (c *Conn) qualifyColumns(query string, rows *command.QueryRows) error {
	if len(rows.Columns) == 0 {
		return nil
	}

	var tableNames []string
	err := c.conn.Raw(func(driverConn any) error {
		ci, ok := driverConn.(interface {
			ColumnInfo(query string) ([]sqlite.ColumnInfo, error)
		})
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		infos, err := ci.ColumnInfo(query)
		if err != nil {
			return err
		}
		tableNames = make([]string, len(rows.Columns))
		for i := range rows.Columns {
			if i < len(infos) {
				tableNames[i] = infos[i].TableName
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i, col := range rows.Columns {
		if tableNames[i] != "" {
			rows.Columns[i] = tableNames[i] + "." + col
		}
	}
	return nil
}

// CopyFrom copies the contents of the database behind src into the database
// behind this connection, using the SQLite online backup API. It can be called
// while changes to the source database are in flight.
func (c *Conn) CopyFrom(src *Conn) error {
	var dstRaw rawConn

	bf := func(driverConn any) error {
		srcRaw, err := rawHandles(driverConn)
		if err != nil {
			return err
		}
		return dstRaw.backupFrom(srcRaw)
	}
	return c.conn.Raw(
		func(driverConn any) error {
			var err error
			dstRaw, err = rawHandles(driverConn)
			if err != nil {
				return err
			}
			return src.conn.Raw(bf)
		})
}

// resultCounts returns the last insert ID and rows affected of a result.
// For a statement with nothing to run (whitespace or a comment) the pure-Go
// driver returns a nil driver result, which database/sql wraps without
// checking, so reading it panics. That case reports zero counts.
func resultCounts(result sql.Result) (lid, ra int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			lid, ra, err = 0, 0, nil
		}
	}()
	if lid, err = result.LastInsertId(); err != nil {
		return 0, 0, err
	}
	if ra, err = result.RowsAffected(); err != nil {
		return 0, 0, err
	}
	return lid, ra, nil
}

// exec runs a statement that returns no rows on the connection.
func (c *Conn) exec(ctx context.Context, query string) error {
	_, err := c.conn.ExecContext(ctx, query)
	return err
}

// Checkpoint runs a WAL checkpoint in the given mode. The busy timeout of
// the connection bounds how long the checkpoint waits for readers.
func (c *Conn) Checkpoint(mode CheckpointMode) (*CheckpointMeta, error) {
	meta := &CheckpointMeta{}
	err := c.conn.QueryRowContext(context.Background(), checkpointPRAGMAs[mode]).Scan(&meta.Code, &meta.Pages, &meta.Moved)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// AutoCheckpoint returns the automatic checkpoint setting of the connection.
func (c *Conn) AutoCheckpoint() (int, error) {
	var n int
	err := c.conn.QueryRowContext(context.Background(), "PRAGMA wal_autocheckpoint").Scan(&n)
	return n, err
}

// BusyTimeout returns the busy timeout of the connection, in milliseconds.
func (c *Conn) BusyTimeout() (int, error) {
	var ms int
	err := c.conn.QueryRowContext(context.Background(), "PRAGMA busy_timeout").Scan(&ms)
	return ms, err
}

// SetBusyTimeout sets the busy timeout of the connection, in milliseconds.
func (c *Conn) SetBusyTimeout(ms int) error {
	return c.exec(context.Background(), fmt.Sprintf("PRAGMA busy_timeout=%d", ms))
}

// SynchronousMode returns the synchronous mode of the connection.
func (c *Conn) SynchronousMode() (SynchronousMode, error) {
	var n int
	if err := c.conn.QueryRowContext(context.Background(), "PRAGMA synchronous").Scan(&n); err != nil {
		return 0, err
	}
	return SynchronousModeFromInt(n)
}

// SetSynchronousMode sets the synchronous mode of the connection.
func (c *Conn) SetSynchronousMode(mode SynchronousMode) error {
	if err := c.exec(context.Background(), fmt.Sprintf("PRAGMA synchronous=%s", mode)); err != nil {
		return fmt.Errorf("failed to set synchronous mode to %s: %s", mode, err.Error())
	}
	return nil
}

// InTransaction returns whether a transaction is open on the connection.
func (c *Conn) InTransaction() (bool, error) {
	var autoCommit bool
	if err := c.conn.Raw(func(driverConn any) error {
		raw, err := rawHandles(driverConn)
		if err != nil {
			return err
		}
		autoCommit = raw.autocommit()
		return nil
	}); err != nil {
		return false, err
	}
	return !autoCommit, nil
}

// pragmas reads the named pragmas on the connection.
func (c *Conn) pragmas(names []string) (map[string]string, error) {
	m := make(map[string]string, len(names))
	for _, p := range names {
		var s string
		if err := c.conn.QueryRowContext(context.Background(), fmt.Sprintf("PRAGMA %s", p)).Scan(&s); err != nil {
			return nil, err
		}
		m[p] = s
	}
	return m, nil
}
