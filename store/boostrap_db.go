package store

import (
	"github.com/rqlite/rqlite/v9/command/proto"
	"github.com/rqlite/rqlite/v9/db"
)

// BootstrapDB represents a database used to serve queries while
// the main database is being rebuilt from the Raft subsystem.
type BootstrapDB struct {
	path string
	db   *db.DB
}

// NewBootstrapDB returns a new BootstrapDB for the given path.
func NewBootstrapDB(path string) (*BootstrapDB, error) {
	return &BootstrapDB{}, nil
}

// Open opens the BootstrapDB.
func (b *BootstrapDB) Open() error {
	db, err := db.Open(b.path, false, false)
	if err != nil {
		return err
	}
	b.db = db
	return nil
}

// Query calls Query on the underlying database.
func (b *BootstrapDB) Query(q *proto.Request, xTime bool) ([]*proto.QueryRows, error) {
	return b.db.Query(q, xTime)
}

// Close closes the underlying database.
func (b *BootstrapDB) Close() error {
	return b.db.Close()
}
