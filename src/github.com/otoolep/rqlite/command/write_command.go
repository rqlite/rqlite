package command

import (
	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/db"
)

// This command encapsulates a sqlite statement.
type WriteCommand struct {
	Stmt string `json:"stmt"`
}

// Creates a new write command.
func NewWriteCommand(stmt string) *WriteCommand {
	return &WriteCommand{
		Stmt: stmt,
	}
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
	return "write"
}

// Executes an sqlite statement.
func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(*db.DB)
	return nil, db.Execute(c.Stmt)
}

// This command encapsulates a set of sqlite statement, which are executed
// within a transaction.
type TransactionWriteCommandSet struct {
	Stmts []string `json:"stmts"`
}

// Creates a new set of sqlite commands, which execute within a transaction.
func NewTransactionWriteCommandSet(stmts []string) *TransactionWriteCommandSet {
	return &TransactionWriteCommandSet{
		Stmts: stmts,
	}
}

// The name of the command in the log.
func (c *TransactionWriteCommandSet) CommandName() string {
	return "transaction_write"
}

// Executes a set of sqlite statements, within a transaction. All statements
// will take effect, or none.
func (c *TransactionWriteCommandSet) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(*db.DB)

	err := db.StartTransaction()
	if err != nil {
		return nil, err
	}
	for i := range c.Stmts {
		err = db.Execute(c.Stmts[i])
		if err != nil {
			err2 := db.RollbackTransaction()
			if err2 != nil {
				// Log err2!
			}
			return nil, err
		}
	}
	err = db.CommitTransaction()
	if err != nil {
		return nil, err
	}
	return nil, nil
}
