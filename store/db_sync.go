package store

import (
	"io"

	"github.com/rqlite/rqlite/v8/command"
	sql "github.com/rqlite/rqlite/v8/db"
)

type DBSync struct {
	*Store
	*sql.DB
}

func (db *DBSync) Stats() (map[string]interface{}, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Stats()
}

func (db *DBSync) Path() string {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Path()
}

func (db *DBSync) WALPath() string {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.WALPath()
}

func (db *DBSync) WALSize() (int64, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.WALSize()
}

func (db *DBSync) Checkpoint() error {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Checkpoint()
}

func (db *DBSync) Query(req *command.Request, xTime bool) ([]*command.QueryRows, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Query(req, xTime)
}

func (db *DBSync) Request(req *command.Request, xTime bool) ([]*command.ExecuteQueryResponse, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Request(req, xTime)
}

func (db *DBSync) Execute(ex *command.Request, xTime bool) ([]*command.ExecuteResult, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Execute(ex, xTime)
}

func (db *DBSync) Backup(path string, vacuum bool) error {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Backup(path, vacuum)
}

func (db *DBSync) Serialize() ([]byte, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Serialize()
}

func (db *DBSync) Dump(w io.Writer) error {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Dump(w)
}

func (db *DBSync) StmtReadOnly(sql string) (bool, error) {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.StmtReadOnly(sql)
}

func (db *DBSync) Close() error {
	db.Store.dbMu.RLock()
	defer db.Store.dbMu.RUnlock()
	return db.DB.Close()
}
