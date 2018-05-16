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
	db    *sdb.Conn // Connection to SQLite database.
	store *Store    // Store to apply commands to.
	id    uint64    // Connection ID, used as a handle by clients.

	timeMu    sync.Mutex
	created   time.Time
	lastUsed  time.Time
	txCreated time.Time

	logger *log.Logger
}

// NewConnection returns a connection to the database.
func NewConnection(c *sdb.Conn, s *Store, id uint64) *Connection {
	return &Connection{
		db:      c,
		store:   s,
		id:      id,
		created: time.Now(),
		logger:  log.New(os.Stderr, "[connection] ", log.LstdFlags),
	}
}

// ID returns the ID of the connection.
func (c *Connection) ID() uint64 {
	return c.id
}

// String implements the Stringer interface on the Connection.
func (c *Connection) String() string {
	return fmt.Sprintf("%d", c.id)
}

// Execute executes queries that return no rows, but do modify the database.
func (c *Connection) Execute(ex *ExecuteRequest) (*ExecuteResponse, error) {
	c.timeMu.Lock()
	c.lastUsed = time.Now()
	c.timeMu.Unlock()
	return c.store.execute(c, ex)
}

// ExecuteOrAbort executes the requests, but aborts any active transaction
// on the underlying database in the case of any error.
func (c *Connection) ExecuteOrAbort(ex *ExecuteRequest) (resp *ExecuteResponse, retErr error) {
	c.timeMu.Lock()
	c.lastUsed = time.Now()
	c.timeMu.Unlock()
	return c.store.executeOrAbort(c, ex)
}

// Query executes queries that return rows, and do not modify the database.
func (c *Connection) Query(qr *QueryRequest) (*QueryResponse, error) {
	c.timeMu.Lock()
	c.lastUsed = time.Now()
	c.timeMu.Unlock()
	return c.store.query(c, qr)
}

// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
// should know exactly what it is doing if it decides to call this function. It
// can be used to clean up any dangling state that may result from certain
// error scenarios.
func (c *Connection) AbortTransaction() error {
	_, err := c.Execute(&ExecuteRequest{[]string{"ROLLBACK"}, false, false})
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	return c.store.disconnect(c)
}
