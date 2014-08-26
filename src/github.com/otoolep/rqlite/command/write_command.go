package command

import (
	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/db"

	log "code.google.com/p/log4go"
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
	log.Debug("Applying WriteCommand: '%s'", c.Stmt)
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
	log.Debug("Applying TransactionWriteCommandSet: %v", c.Stmts)
	db := server.Context().(*db.DB)

	err := db.StartTransaction()
	if err != nil {
		log.Error("Failed to start transaction:", err.Error())
		return nil, err
	}
	for i := range c.Stmts {
		err = db.Execute(c.Stmts[i])
		if err != nil {
			log.Error("Failed to execute statement within transaction", err.Error())
			err2 := db.RollbackTransaction()
			if err2 != nil {
				log.Error("Failed to rollback transaction:", err2.Error())
			}
			return nil, err
		}
	}
	err = db.CommitTransaction()
	if err != nil {
		log.Error("Failed to commit transaction:", err.Error())
		return nil, err
	}
	return nil, nil
}
