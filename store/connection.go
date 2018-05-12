package store

import (
	"fmt"
	"log"
	"os"

	sdb "github.com/rqlite/rqlite/db"
)

type Connection struct {
	db    *sdb.Conn // Connection to SQLite database.
	store *Store    // Store to apply commands to.
	id    uint64    // Connection ID, used as a handle by clients.

	logger *log.Logger
}

func NewConnection(c *sdb.Conn, s *Store, id uint64) *Connection {
	return &Connection{
		db:     c,
		store:  s,
		id:     id,
		logger: log.New(os.Stderr, "[connection] ", log.LstdFlags),
	}
}

func (c *Connection) ID() uint64 {
	return c.id
}

func (c *Connection) String() string {
	return fmt.Sprintf("%d", c.id)
}

// Execute executes queries that return no rows, but do modify the database.
func (c *Connection) Execute(ex *ExecuteRequest) (*ExecuteResponse, error) {
	return c.store.execute(c, ex)
}

// ExecuteOrAbort executes the requests, but aborts any active transaction
// on the underlying database in the case of any error.
func (c *Connection) ExecuteOrAbort(ex *ExecuteRequest) (resp *ExecuteResponse, retErr error) {
	defer func() {
		var errored bool
		for i := range resp.Results {
			if resp.Results[i].Error != "" {
				errored = true
				break
			}
		}
		if retErr != nil || errored {
			if err := c.AbortTransaction(); err != nil {
				c.logger.Printf("WARNING: failed to abort transaction on connection %d: %s",
					c.id, err.Error())
			}
		}
	}()
	return c.store.execute(c, ex)
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
	_, err := c.Execute(&ExecuteRequest{[]string{"ROLLBACK"}, false, false})
	return err
}

func (c *Connection) Close() error {
	// Tell store conn is dead.
	return nil
}
