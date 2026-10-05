package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"
)

// ConnectionFactory opens every connection of a single database/sql
// connection pool. It implements driver.Connector, so a pool is created by
// passing a factory to sql.OpenDB. database/sql then calls Connect whenever it
// needs a connection: the first one, an additional one, or a replacement for
// one it has discarded.
//
// A factory is the single place where a connection is configured. A
// connection's configuration comes from the DSN and DriverConfig, which are
// fixed when the factory is created, and from the factory's settings, which
// can be changed at any time. The factory keeps track of every connection it
// has opened which has not yet been closed, so a change to a setting is
// applied to all of those connections, and to every connection opened
// afterwards.
//
// A factory does not know, or care, whether its connections are read-write or
// read-only. That is determined solely by the DSN.
type ConnectionFactory struct {
	drv *sqlite3.SQLiteDriver
	cfg DriverConfig
	dsn string

	mu    sync.Mutex
	conns map[*Connection]struct{}

	// Settings. A nil value means the setting has not been set, and
	// connections are left as the DSN and DriverConfig configured them.
	busyTimeout   *int
	synchronous   *SynchronousMode
	preUpdateHook func(sqlite3.SQLitePreUpdateData)
	updateHook    func(int, string, string, int64)
	commitHook    func() int
	rollbackHook  func()
}

// NewConnectionFactory returns a factory which opens connections using the
// given DSN, and configures each of them according to cfg.
func NewConnectionFactory(dsn string, cfg *DriverConfig) *ConnectionFactory {
	return &ConnectionFactory{
		drv:   &sqlite3.SQLiteDriver{Extensions: cfg.Extensions},
		cfg:   *cfg,
		dsn:   dsn,
		conns: make(map[*Connection]struct{}),
	}
}

// Connect opens, configures, and returns a new connection. It implements
// driver.Connector.
func (f *ConnectionFactory) Connect(context.Context) (driver.Conn, error) {
	dc, err := f.drv.Open(f.dsn)
	if err != nil {
		return nil, err
	}
	conn := &Connection{SQLiteConn: dc.(*sqlite3.SQLiteConn), factory: f}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.configure(conn); err != nil {
		conn.SQLiteConn.Close()
		return nil, err
	}
	f.conns[conn] = struct{}{}
	return conn, nil
}

// Driver returns the underlying SQLite driver. It implements driver.Connector.
func (f *ConnectionFactory) Driver() driver.Driver {
	return f.drv
}

// NumConnections returns the number of connections the factory has opened
// which have not yet been closed.
func (f *ConnectionFactory) NumConnections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// SetBusyTimeout sets the busy timeout, in milliseconds, of all current and
// future connections.
func (f *ConnectionFactory) SetBusyTimeout(ms int) error {
	if ms < 0 {
		return fmt.Errorf("invalid busy timeout %d", ms)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.busyTimeout = &ms
	return f.forEach(f.applyBusyTimeout)
}

// SetSynchronousMode sets the synchronous mode of all current and future
// connections. SQLite does not allow the mode of a connection to be changed
// while that connection has a transaction open, so in that case an error is
// returned and that connection is left in its previous mode.
func (f *ConnectionFactory) SetSynchronousMode(mode SynchronousMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synchronous = &mode
	return f.forEach(f.applySynchronous)
}

// RegisterPreUpdateHook registers a preupdate hook on all current and future
// connections, replacing any existing hook. If hook is nil the hook is removed.
func (f *ConnectionFactory) RegisterPreUpdateHook(hook func(sqlite3.SQLitePreUpdateData)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.preUpdateHook = hook
	f.forEach(f.applyHooks)
}

// RegisterUpdateHook registers an update hook on all current and future
// connections, replacing any existing hook. If hook is nil the hook is removed.
func (f *ConnectionFactory) RegisterUpdateHook(hook func(int, string, string, int64)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateHook = hook
	f.forEach(f.applyHooks)
}

// RegisterCommitHook registers a commit hook on all current and future
// connections, replacing any existing hook. If hook is nil the hook is removed.
func (f *ConnectionFactory) RegisterCommitHook(hook func() int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitHook = hook
	f.forEach(f.applyHooks)
}

// RegisterRollbackHook registers a rollback hook on all current and future
// connections, replacing any existing hook. If hook is nil the hook is removed.
func (f *ConnectionFactory) RegisterRollbackHook(hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rollbackHook = hook
	f.forEach(f.applyHooks)
}

// forEach calls fn for every current connection, returning any errors. A
// setting is recorded by the factory before forEach is called, so an error
// means the setting could not be applied to at least one current connection,
// but it will still be applied to every future connection. The caller must
// hold the mutex.
func (f *ConnectionFactory) forEach(fn func(c *Connection) error) error {
	var errs []error
	for c := range f.conns {
		if err := fn(c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// configure applies the DriverConfig, and then the settings, to a new
// connection. The caller must hold the mutex.
func (f *ConnectionFactory) configure(c *Connection) error {
	// Checkpoint-on-close configuration.
	if f.cfg.ChkOnClose == CnkOnCloseModeDisabled {
		if err := c.DBConfigNoCkptOnClose(); err != nil {
			return fmt.Errorf("cannot disable checkpoint on close: %w", err)
		}
	}

	// It's critical that rqlite has full control over the checkpointing process
	// so disable all auto-checkpoint. This doesn't return an error on a read-only
	// connection, so an error here really is an issue.
	if _, err := c.Exec("PRAGMA wal_autocheckpoint=0", nil); err != nil {
		return fmt.Errorf("failed to disable automatic checkpointing: %w", err)
	}

	// Foreign key constraints.
	if f.cfg.ForeignKeys {
		if _, err := c.Exec("PRAGMA foreign_keys = ON", nil); err != nil {
			return fmt.Errorf("cannot enable foreign keys: %w", err)
		}
	}

	// Query tracing.
	if f.cfg.QueryLogger != nil {
		if err := c.SetTrace(&sqlite3.TraceConfig{
			Callback:        f.cfg.QueryLogger.TraceHook,
			EventMask:       sqlite3.TraceStmt | sqlite3.TraceProfile,
			WantExpandedSQL: true,
		}); err != nil {
			return err
		}
	}

	// Settings.
	if err := f.applyBusyTimeout(c); err != nil {
		return err
	}
	if err := f.applySynchronous(c); err != nil {
		return err
	}
	return f.applyHooks(c)
}

func (f *ConnectionFactory) applyBusyTimeout(c *Connection) error {
	if f.busyTimeout == nil {
		return nil
	}
	if _, err := c.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", *f.busyTimeout), nil); err != nil {
		return fmt.Errorf("failed to set busy timeout: %w", err)
	}
	return nil
}

func (f *ConnectionFactory) applySynchronous(c *Connection) error {
	if f.synchronous == nil {
		return nil
	}
	if _, err := c.Exec(fmt.Sprintf("PRAGMA synchronous=%s", *f.synchronous), nil); err != nil {
		return fmt.Errorf("failed to set synchronous mode to %s: %w", *f.synchronous, err)
	}
	return nil
}

func (f *ConnectionFactory) applyHooks(c *Connection) error {
	c.RegisterPreUpdateHook(f.preUpdateHook)
	c.RegisterUpdateHook(f.updateHook)
	c.RegisterCommitHook(f.commitHook)
	c.RegisterRollbackHook(f.rollbackHook)
	return nil
}

// remove stops the factory tracking the given connection.
func (f *ConnectionFactory) remove(c *Connection) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.conns, c)
}

// Connection is a connection opened by a ConnectionFactory. It is a SQLite
// connection which knows the factory it came from, so that the factory stops
// tracking it when it is closed.
//
// database/sql passes a *Connection to the function given to sql.Conn.Raw.
type Connection struct {
	*sqlite3.SQLiteConn
	factory *ConnectionFactory
}

// Close closes the connection. The factory no longer applies changes to its
// settings to a closed connection.
func (c *Connection) Close() error {
	c.factory.remove(c)
	return c.SQLiteConn.Close()
}
