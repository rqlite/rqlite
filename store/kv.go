package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// KVTableName is the name of the SQLite table that backs the KV API.
const KVTableName = "__rqlite_kv"

// SQL statements used by the KV API. Exported so callers (tests, HTTP layer)
// can reference them by name rather than duplicating string literals.
const (
	KVCreateTableSQL = `CREATE TABLE IF NOT EXISTS __rqlite_kv (
		k TEXT PRIMARY KEY NOT NULL,
		v BLOB NOT NULL
	) WITHOUT ROWID`

	KVUpsertSQL = `INSERT INTO __rqlite_kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v`

	KVSelectSQL = `SELECT v FROM __rqlite_kv WHERE k = ?`

	KVDeleteSQL = `DELETE FROM __rqlite_kv WHERE k = ?`

	KVCountSQL = `SELECT COUNT(*) FROM __rqlite_kv`

	KVDropSQL = `DROP TABLE __rqlite_kv`
)

var (
	// ErrKVKeyNotFound is returned when GetKey or DeleteKey is called for a
	// key that does not exist (or when the backing table has never been created).
	ErrKVKeyNotFound = errors.New("kv key not found")

	// ErrKVEmptyKey is returned when an empty key is supplied.
	ErrKVEmptyKey = errors.New("kv key must be non-empty")
)

// SetKey upserts key→value into the KV table, lazily creating the table on
// first use. Returns the Raft index of the committed log entry.
func (s *Store) SetKey(ctx context.Context, key string, value []byte) (uint64, error) {
	if key == "" {
		return 0, ErrKVEmptyKey
	}

	s.kvMu.Lock()
	defer s.kvMu.Unlock()

	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction:     true,
			RollbackOnError: true,
			Statements: []*proto.Statement{
				{Sql: KVCreateTableSQL},
				{
					Sql: KVUpsertSQL,
					Parameters: []*proto.Parameter{
						{Value: &proto.Parameter_S{S: key}},
						{Value: &proto.Parameter_Y{Y: value}},
					},
				},
			},
		},
	}

	results, idx, err := s.Execute(ctx, er)
	if err != nil {
		return 0, err
	}
	if err := firstStatementError(results); err != nil {
		return 0, err
	}
	return idx, nil
}

// GetKey returns the value stored under key at the requested consistency
// level. Returns ErrKVKeyNotFound if the key (or the backing table) does
// not exist.
func (s *Store) GetKey(ctx context.Context, key string, level proto.ConsistencyLevel,
	freshness int64, freshnessStrict bool, linearizableTimeout int64) ([]byte, uint64, error) {
	if key == "" {
		return nil, 0, ErrKVEmptyKey
	}

	qr := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: KVSelectSQL,
					Parameters: []*proto.Parameter{
						{Value: &proto.Parameter_S{S: key}},
					},
				},
			},
		},
		Level:               level,
		Freshness:           freshness,
		FreshnessStrict:     freshnessStrict,
		LinearizableTimeout: linearizableTimeout,
	}

	rows, _, idx, err := s.Query(ctx, qr)
	if err != nil {
		if isNoSuchTableErr(err) {
			return nil, 0, ErrKVKeyNotFound
		}
		return nil, 0, err
	}
	if len(rows) == 0 {
		return nil, idx, ErrKVKeyNotFound
	}
	if rows[0].Error != "" {
		if strings.Contains(rows[0].Error, "no such table") {
			return nil, idx, ErrKVKeyNotFound
		}
		return nil, idx, errors.New(rows[0].Error)
	}
	if len(rows[0].Values) == 0 || len(rows[0].Values[0].Parameters) == 0 {
		return nil, idx, ErrKVKeyNotFound
	}
	return paramAsBytes(rows[0].Values[0].Parameters[0]), idx, nil
}

// DeleteKey removes the row for key. If it was the last key in the table,
// the backing table is also dropped. Returns ErrKVKeyNotFound if the key
// (or table) was not present. Returns the Raft index of the DELETE.
func (s *Store) DeleteKey(ctx context.Context, key string) (uint64, error) {
	if key == "" {
		return 0, ErrKVEmptyKey
	}

	s.kvMu.Lock()
	defer s.kvMu.Unlock()

	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{
				{
					Sql: KVDeleteSQL,
					Parameters: []*proto.Parameter{
						{Value: &proto.Parameter_S{S: key}},
					},
				},
			},
		},
	}
	results, idx, err := s.Execute(ctx, er)
	if err != nil {
		if isNoSuchTableErr(err) {
			return 0, ErrKVKeyNotFound
		}
		return 0, err
	}
	if len(results) == 0 {
		return 0, fmt.Errorf("kv delete: no result returned")
	}
	if errStr := results[0].GetError(); errStr != "" {
		if strings.Contains(errStr, "no such table") {
			return 0, ErrKVKeyNotFound
		}
		return 0, errors.New(errStr)
	}
	res := results[0].GetE()
	if res == nil {
		return 0, fmt.Errorf("kv delete: missing execute result")
	}
	if res.Error != "" {
		if strings.Contains(res.Error, "no such table") {
			return 0, ErrKVKeyNotFound
		}
		return 0, errors.New(res.Error)
	}
	if res.RowsAffected == 0 {
		return 0, ErrKVKeyNotFound
	}

	// The key was removed. Count what remains and drop the table if empty.
	if err := s.maybeDropKVTable(ctx); err != nil {
		return idx, err
	}
	return idx, nil
}

// maybeDropKVTable issues a COUNT and, if the table is empty, drops it.
// Caller must hold s.kvMu.
func (s *Store) maybeDropKVTable(ctx context.Context) error {
	countReq := &proto.QueryRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{{Sql: KVCountSQL}},
		},
		Level: proto.ConsistencyLevel_NONE,
	}
	rows, _, _, err := s.Query(ctx, countReq)
	if err != nil {
		return err
	}
	if len(rows) == 0 || rows[0].Error != "" {
		if len(rows) > 0 && rows[0].Error != "" {
			return errors.New(rows[0].Error)
		}
		return nil
	}
	if len(rows[0].Values) == 0 || len(rows[0].Values[0].Parameters) == 0 {
		return nil
	}
	count, ok := paramAsInt(rows[0].Values[0].Parameters[0])
	if !ok {
		return nil
	}
	if count > 0 {
		return nil
	}

	dropReq := &proto.ExecuteRequest{
		Request: &proto.Request{
			Statements: []*proto.Statement{{Sql: KVDropSQL}},
		},
	}
	dropResults, _, err := s.Execute(ctx, dropReq)
	if err != nil {
		return err
	}
	if err := firstStatementError(dropResults); err != nil {
		return err
	}
	return nil
}

// firstStatementError returns the first non-empty error embedded in an
// ExecuteRequest response, mirroring how rqlite surfaces per-statement
// errors through the oneof result.
func firstStatementError(results []*proto.ExecuteQueryResponse) error {
	for _, r := range results {
		if errStr := r.GetError(); errStr != "" {
			return errors.New(errStr)
		}
		if e := r.GetE(); e != nil && e.Error != "" {
			return errors.New(e.Error)
		}
	}
	return nil
}

func isNoSuchTableErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no such table")
}

func paramAsBytes(p *proto.Parameter) []byte {
	if p == nil {
		return nil
	}
	switch v := p.GetValue().(type) {
	case *proto.Parameter_Y:
		return v.Y
	case *proto.Parameter_S:
		return []byte(v.S)
	}
	return nil
}

func paramAsInt(p *proto.Parameter) (int64, bool) {
	if p == nil {
		return 0, false
	}
	switch v := p.GetValue().(type) {
	case *proto.Parameter_I:
		return v.I, true
	}
	return 0, false
}
