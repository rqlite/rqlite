// Package sql provides an way to create sql.DB objects out of an RQLite store.
//
// The goal of this package is to provide an interface that looks and feels
// identical to a plain old sql.DB one. The key here is really the
// RqliteSQLWrapper interface: users create a store, and provide two functions
// which execute queries on the leader (using whatever transport API they have
// defined), and users can use this Store like a regular sql.DB
package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hashicorp/raft"

	"github.com/rqlite/rqlite/db"
	"github.com/rqlite/rqlite/store"
)

// RqliteSQLWrapper is the user-supplied interface that allows the driver to
// resolve the rqlite store and communicate with its members.
type RqliteSQLWrapper interface {
	// Store returns the local store that this wrapper wraps.
	Store() (*store.Store, error)

	// ExecOn should execute the specified query on the specified address
	// in the cluster (which for now is always the leader's address) and
	// return the result.
	ExecOn(addr, query string) (*db.Result, error)

	// QueryOn is similar to ExecOn, in that it should run the query on the
	// specified address with the query and return the result.
	QueryOn(addr, query string) (*db.Rows, error)
}

var rqDriver *RqliteDriver

func init() {
	rqDriver = &RqliteDriver{}
	rqDriver.wrappers = map[string]RqliteSQLWrapper{}

	sql.Register("rqlite", rqDriver)
}

func isNotLeaderErr(err error) bool {
	return err == store.ErrNotLeader || err == raft.ErrNotLeader

}

// RegisterStore registers a store callback with the sql driver under the
// specified name. This makes it available via sql.Open("rqlite", "...").
func RegisterStore(name string, w RqliteSQLWrapper) {
	rqDriver.wrappers[name] = w
}

type RqliteResult struct {
	result *db.Result
}

func (r *RqliteResult) LastInsertId() (int64, error) {
	if r.result.Error != "" {
		return -1, fmt.Errorf(r.result.Error)
	}

	return r.result.LastInsertID, nil
}

func (r *RqliteResult) RowsAffected() (int64, error) {
	if r.result.Error != "" {
		return -1, fmt.Errorf(r.result.Error)
	}

	return r.result.RowsAffected, nil
}

type RqliteRows struct {
	rows *db.Rows
	cur  int
}

func (r *RqliteRows) Columns() []string {
	return r.rows.Columns
}

func (r *RqliteRows) Close() error {
	/* no-op */
	return nil
}

func (r *RqliteRows) Next(dest []driver.Value) error {
	if r.cur >= len(r.rows.Values) {
		return io.EOF
	}

	row := r.rows.Values[r.cur]
	r.cur++

	for i := range dest {
		dest[i] = row[i]
	}

	return nil
}

type RqliteStmt struct {
	q    string
	conn *RqliteConn
}

func (s *RqliteStmt) Close() error {
	/* no-op */
	return nil
}

func (s *RqliteStmt) NumInput() int {
	return strings.Count(s.q, "?")
}

func render(q string, args []driver.Value) (string, error) {
	/* TODO: we should probably do some more escaping and stuff here */
	rendered := []interface{}{}
	for _, v := range args {
		r := ""
		switch v.(type) {
		case int64:
			r = fmt.Sprintf("%v", v)
		case float64:
			r = fmt.Sprintf("%v", v)
		case bool:
			r = fmt.Sprintf("%v", v)
		case time.Time:
			r = fmt.Sprintf(`'%v'`, v)
		case []byte:
			r = fmt.Sprintf(`'%s'`, v)
		case string:
			r = fmt.Sprintf(`'%s'`, v)
		default:
			return "", fmt.Errorf("unknown type %T", v)
		}
		rendered = append(rendered, r)
	}

	q = strings.Replace(q, "%", "%%", -1)
	q = strings.Replace(q, "?", "%s", -1)
	q = fmt.Sprintf(q, rendered...)
	return q, nil
}

func (s *RqliteStmt) Exec(args []driver.Value) (driver.Result, error) {
	q, err := render(s.q, args)
	if err != nil {
		return nil, err
	}

	results, err := s.conn.store.Execute([]string{q}, false, false)
	if isNotLeaderErr(err) {
		result, err := s.conn.wrapper.ExecOn(s.conn.store.Leader(), q)
		if err != nil {
			return nil, err
		}

		return &RqliteResult{result}, nil
	} else if err != nil {
		return nil, err
	}

	if len(results) != 1 {
		return nil, fmt.Errorf("wrong number of results, got %d", len(results))
	}

	if results[0].Error != "" {
		return nil, fmt.Errorf(results[0].Error)
	}

	return &RqliteResult{results[0]}, err
}

func (s *RqliteStmt) Query(args []driver.Value) (driver.Rows, error) {
	q, err := render(s.q, args)
	if err != nil {
		return nil, err
	}

	result, err := s.conn.store.Query([]string{q}, false, false, store.Weak)
	if isNotLeaderErr(err) {
		rows, err := s.conn.wrapper.QueryOn(s.conn.store.Leader(), q)
		if err != nil {
			return nil, err
		}

		return &RqliteRows{rows: rows}, nil

	} else if err != nil {
		return nil, err
	}

	if len(result) != 1 {
		return nil, fmt.Errorf("wrong number of rows, expected %d", len(result))
	}

	return &RqliteRows{rows: result[0]}, nil
}

type RqliteConn struct {
	wrapper RqliteSQLWrapper
	store   *store.Store
	closed  bool
}

func (c *RqliteConn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, fmt.Errorf("connection is closed")
	}
	return &RqliteStmt{query, c}, nil
}

func (c *RqliteConn) Close() error {
	if c.closed {
		return fmt.Errorf("connection is closed")
	}

	c.closed = true
	return nil
}

func (c *RqliteConn) Begin() (driver.Tx, error) {
	return c.store.Begin()
}

type RqliteDriver struct {
	wrappers map[string]RqliteSQLWrapper
}

func (d *RqliteDriver) Open(name string) (driver.Conn, error) {
	w, ok := d.wrappers[name]
	if !ok {
		return nil, fmt.Errorf("%s is not registered with the rqlite sql driver", name)
	}

	store, err := w.Store()
	if err != nil {
		return nil, err
	}

	return &RqliteConn{wrapper: w, store: store}, nil
}
