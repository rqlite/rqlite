package store

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	sdb "github.com/rqlite/rqlite/db"
)

// Connection is a connection to the database.
type Connection struct {
	db     *sdb.Conn // Connection to SQLite database.
	store  *Store    // Store to apply commands to.
	ConnID uint64    `json:"id,omitempty"` // Connection ID, used as a handle by clients.

	timeMu     sync.Mutex
	CreatedAt  time.Time `json:"created_at,omitempty"`
	LastUsedAt time.Time `json:"last_used_at,omitempty"`

	txStateMu   sync.Mutex
	TxStartedAt time.Time `json:"tx_started_at,omitempty"`

	logger *log.Logger
}

// NewConnection returns a connection to the database.
func NewConnection(c *sdb.Conn, s *Store, id uint64) *Connection {
	return &Connection{
		db:        c,
		store:     s,
		ConnID:    id,
		CreatedAt: time.Now(),
		logger:    log.New(os.Stderr, "[connection] ", log.LstdFlags),
	}
}

// Restore prepares a partially ready connection.
func (c *Connection) Restore(dbConn *sdb.Conn, s *Store) {
	c.db = dbConn
	c.store = s
	c.logger = log.New(os.Stderr, "[connection] ", log.LstdFlags)
}

// SetLastUsedNow marks the connection as being used now.
func (c *Connection) SetLastUsedNow() {
	c.timeMu.Lock()
	c.LastUsedAt = time.Now()
	c.timeMu.Unlock()
}

// String implements the Stringer interface on the Connection.
func (c *Connection) String() string {
	return fmt.Sprintf("connection:%d", c.ConnID)
}

// ID returns the connection ID.
func (c *Connection) ID() uint64 {
	return c.ConnID
}

// TransactionActive returns whether a transaction is active on the connection.
func (c *Connection) TransactionActive() bool {
	return c.db.TransactionActive()
}

// Execute executes queries that return no rows, but do modify the database.
func (c *Connection) Execute(ex *ExecuteRequest) (*ExecuteResponse, error) {
	return c.store.execute(c, ex)
}

// ExecuteOrAbort executes the requests, but aborts any active transaction
// on the underlying database in the case of any error.
func (c *Connection) ExecuteOrAbort(ex *ExecuteRequest) (resp *ExecuteResponse, retErr error) {
	return c.store.executeOrAbort(c, ex)
}

// Query executes queries that return rows, and do not modify the database.
func (c *Connection) Query(qr *QueryRequest) (*QueryResponse, error) {
	return c.store.query(c, qr)
}

// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
// should know exactly what it is doing if it decides to call this function. It
// can be used to clean up any dangling state that may result from certain
// error scenarios.
func (c *Connection) AbortTransaction() error {
	_, err := c.store.execute(c, &ExecuteRequest{[]string{"ROLLBACK"}, false, false})
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	return c.store.disconnect(c)
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
