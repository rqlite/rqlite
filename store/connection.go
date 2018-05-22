package store

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	sdb "github.com/rqlite/rqlite/db"
)

const pollPeriod = time.Second

type ConnectionOptions struct {
	IdleTimeout time.Duration
	TxTimeout   time.Duration
}

// Connection is a connection to the database.
type Connection struct {
	ID uint64 `json:"id,omitempty"` // Connection ID, used as a handle by clients.

	dbMu  sync.RWMutex
	db    *sdb.Conn // Connection to SQLite database.
	store *Store    // Store to apply commands to.

	timeMu      sync.Mutex
	CreatedAt   time.Time     `json:"created_at,omitempty"`
	LastUsedAt  time.Time     `json:"last_used_at,omitempty"`
	IdleTimeout time.Duration `json:"idle_timeout,omitempty"`
	TxTimeout   time.Duration `json:"tx_timeout,omitempty"`

	txStateMu   sync.Mutex
	TxStartedAt time.Time `json:"tx_started_at,omitempty"`

	wg   sync.WaitGroup
	done chan struct{}

	logPrefix string
	logger    *log.Logger
}

// NewConnection returns a connection to the database.
func NewConnection(c *sdb.Conn, s *Store, id uint64, it, tt time.Duration) *Connection {
	conn := Connection{
		db:          c,
		store:       s,
		ID:          id,
		CreatedAt:   time.Now(),
		IdleTimeout: it,
		TxTimeout:   tt,
		done:        make(chan struct{}, 1),
		logger:      log.New(os.Stderr, connectionLogPrefix(id), log.LstdFlags),
	}
	conn.run(conn.done)
	return &conn
}

// Restore prepares a partially ready connection.
func (c *Connection) Restore(dbConn *sdb.Conn, s *Store) {
	c.db = dbConn
	c.store = s
	c.logger = log.New(os.Stderr, connectionLogPrefix(c.ID), log.LstdFlags)
}

// SetLastUsedNow marks the connection as being used now.
func (c *Connection) SetLastUsedNow() {
	c.timeMu.Lock()
	defer c.timeMu.Unlock()
	c.LastUsedAt = time.Now()
}

// String implements the Stringer interface on the Connection.
func (c *Connection) String() string {
	return fmt.Sprintf("connection:%d", c.ID)
}

// TransactionActive returns whether a transaction is active on the connection.
func (c *Connection) TransactionActive() bool {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	return c.db.TransactionActive()
}

// Execute executes queries that return no rows, but do modify the database.
func (c *Connection) Execute(ex *ExecuteRequest) (*ExecuteResponse, error) {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	return c.store.execute(c, ex)
}

// ExecuteOrAbort executes the requests, but aborts any active transaction
// on the underlying database in the case of any error.
func (c *Connection) ExecuteOrAbort(ex *ExecuteRequest) (resp *ExecuteResponse, retErr error) {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	return c.store.executeOrAbort(c, ex)
}

// Query executes queries that return rows, and do not modify the database.
func (c *Connection) Query(qr *QueryRequest) (*QueryResponse, error) {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	return c.store.query(c, qr)
}

// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
// should know exactly what it is doing if it decides to call this function. It
// can be used to clean up any dangling state that may result from certain
// error scenarios.
func (c *Connection) AbortTransaction() error {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	_, err := c.store.execute(c, &ExecuteRequest{[]string{"ROLLBACK"}, false, false})
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.dbMu.Lock()
	defer c.dbMu.Unlock()
	close(c.done)
	c.wg.Wait()
	if c.store != nil {
		if err := c.store.disconnect(c); err != nil {
			return err
		}
	}
	return nil
}

// run starts the goroutine that periodically checks if any active transaction
// on the connection should be aborted, or if the connection should be closed.
func (c *Connection) run(done chan struct{}) {
	c.wg.Add(1)
	defer c.wg.Done()

	ticker := time.NewTicker(pollPeriod)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				c.timeMu.Lock()
				lua := c.LastUsedAt
				c.timeMu.Unlock()
				if time.Since(lua) > c.IdleTimeout && c.IdleTimeout != 0 {
					if err := c.Close(); err != nil {
						c.logger.Printf("failed to close %s:", err.Error())
					} else {
						c.logger.Println("closed due to idle timeout")
					}
				}
				c.txStateMu.Lock()
				tsa := c.TxStartedAt
				c.txStateMu.Unlock()
				if time.Since(tsa) > c.TxTimeout && c.TxTimeout != 0 {
					if err := c.AbortTransaction(); err != nil {
						c.logger.Printf("failed to abort transaction %s:", err.Error())
					} else {
						c.logger.Println("transaction aborted due to transaction timeout")
					}
				}
			}
		}
	}()
}

func connectionLogPrefix(id uint64) string {
	return fmt.Sprintf("[connection:%d] ", id)
}

// TxStateChange is a helper that detects when the transaction state on a
// connection changes.
type TxStateChange struct {
	c    *Connection
	tx   bool
	done bool
}

// NewTxStateChange returns an initialized TxStateChange
func NewTxStateChange(c *Connection) *TxStateChange {
	return &TxStateChange{
		c:  c,
		tx: c.TransactionActive(),
	}
}

// CheckAndSet sets whether a transaction has begun or ended on the
// connection since the TxStateChange was created. Once CheckAndSet
// has been called, this function will panic if called a second time.
func (t *TxStateChange) CheckAndSet() {
	t.c.txStateMu.Lock()
	defer t.c.txStateMu.Unlock()
	defer func() { t.done = true }()

	if t.done {
		panic("CheckAndSet should only be called once")
	}

	if !t.tx && t.c.TransactionActive() && t.c.TxStartedAt.IsZero() {
		t.c.TxStartedAt = time.Now()
	} else if t.tx && !t.c.TransactionActive() && !t.c.TxStartedAt.IsZero() {
		t.c.TxStartedAt = time.Time{}
	}
}
