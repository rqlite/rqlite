package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// pool is where connections of one role come from. It owns a database/sql
// pool built from one connector, so every connection it hands out was
// opened the same way.
type pool struct {
	sqlDB  *sql.DB
	dsn    string
	logger *log.Logger
}

// acquire borrows a connection. The caller must close it.
func (p *pool) acquire(ctx context.Context) (*Conn, error) {
	conn, err := p.sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(conn, p.logger), nil
}

// close closes the pool and every connection in it.
func (p *pool) close() error {
	return p.sqlDB.Close()
}

// poolStats returns the pool statistics.
func (p *pool) poolStats() *PoolStats {
	s := p.sqlDB.Stats()
	return &PoolStats{
		MaxOpenConnections: s.MaxOpenConnections,
		OpenConnections:    s.OpenConnections,
		InUse:              s.InUse,
		Idle:               s.Idle,
		WaitCount:          s.WaitCount,
		WaitDuration:       s.WaitDuration,
		MaxIdleClosed:      s.MaxIdleClosed,
		MaxIdleTimeClosed:  s.MaxIdleTimeClosed,
		MaxLifetimeClosed:  s.MaxLifetimeClosed,
	}
}

// busyTimeout returns the busy timeout of a connection from the pool.
func (p *pool) busyTimeout() (int, error) {
	conn, err := p.acquire(context.Background())
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return conn.BusyTimeout()
}

// setBusyTimeout sets the busy timeout on a connection from the pool.
func (p *pool) setBusyTimeout(ms int) error {
	conn, err := p.acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.SetBusyTimeout(ms)
}

// pragmas reads the named pragmas on one connection from the pool.
func (p *pool) pragmas(names []string) (map[string]string, error) {
	conn, err := p.acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.pragmas(names)
}

// inTransaction returns whether a connection from the pool has a
// transaction open.
func (p *pool) inTransaction() (bool, error) {
	conn, err := p.acquire(context.Background())
	if err != nil {
		return false, err
	}
	defer conn.Close()
	return conn.InTransaction()
}

// writer is the source of the one read-write connection. It serializes
// writes by holding its pool to a single connection, and it is the
// connection that creates the WAL, checkpoints, and runs every statement
// that changes the database.
type writer struct {
	pool
}

// openWriter opens the read-write side of the database at path. After it
// returns the SQLite file exists and, if WAL mode was requested, so do the
// WAL files.
func openWriter(cfg *ConnConfig, path string, wal bool, logger *log.Logger) (retW *writer, retErr error) {
	dsn := MakeDSN(path, ModeReadWrite, cfg.ForeignKeys, wal)
	sqlDB := sql.OpenDB(cfg.connector(dsn))
	sqlDB.SetConnMaxLifetime(0)
	sqlDB.SetMaxOpenConns(1) // Serialize writes on a single connection.
	defer func() {
		if retErr != nil {
			sqlDB.Close()
		}
	}()
	w := &writer{pool{sqlDB: sqlDB, dsn: dsn, logger: logger}}

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping on-disk database: %s", err.Error())
	}
	if wal && !fsutil.FileExists(WALPath(path)) {
		if err := w.createWAL(); err != nil {
			return nil, err
		}
	}
	return w, nil
}

// createWAL forces creation of the WAL files so any external read-only
// connections can read the database. See
// https://www.sqlite.org/draft/wal.html, section 5. It all takes place on
// the same connection.
func (w *writer) createWAL() error {
	conn, err := w.acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.exec(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if err := conn.exec(context.Background(), "ROLLBACK"); err != nil {
		return err
	}
	meta, err := conn.Checkpoint(CheckpointTruncate)
	if err != nil {
		return err
	}
	if !meta.Success() {
		return fmt.Errorf("failed to completely checkpoint WAL at open (%s)", meta)
	}
	return nil
}

// readerPool is the source of read-only connections. Readers run alongside
// the writer, so the pool may hold any number of them.
type readerPool struct {
	pool
}

// openReaderPool opens the read-only side of the database at path. No
// connection is made until one is acquired.
func openReaderPool(cfg *ConnConfig, path string, wal bool, logger *log.Logger) *readerPool {
	dsn := MakeDSN(path, ModeReadOnly, cfg.ForeignKeys, wal)
	sqlDB := sql.OpenDB(cfg.connector(dsn))
	sqlDB.SetConnMaxIdleTime(30 * time.Second)
	sqlDB.SetConnMaxLifetime(0)
	return &readerPool{pool{sqlDB: sqlDB, dsn: dsn, logger: logger}}
}

// setMaxConns sets the maximum number of read-only connections. See
// https://pkg.go.dev/database/sql#DB.SetMaxOpenConns for full details.
func (p *readerPool) setMaxConns(n int) {
	p.sqlDB.SetMaxOpenConns(n)
}
