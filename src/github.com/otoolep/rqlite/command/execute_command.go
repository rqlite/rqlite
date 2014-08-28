package command

import (
	"github.com/otoolep/raft"
	"github.com/otoolep/rqlite/db"

	log "code.google.com/p/log4go"
)

// This command encapsulates a sqlite statement.
type ExecuteCommand struct {
	Stmt string `json:"stmt"`
}

// Creates a new write command.
func NewExecuteCommand(stmt string) *ExecuteCommand {
	return &ExecuteCommand{
		Stmt: stmt,
	}
}

// The name of the command in the log.
func (c *ExecuteCommand) CommandName() string {
	return "write"
}

// Executes an sqlite statement.
func (c *ExecuteCommand) Apply(server raft.Server) (interface{}, error) {
	log.Trace("Applying ExecuteCommand: '%s'", c.Stmt)
	db := server.Context().(*db.DB)
	return nil, db.Execute(c.Stmt)
}

// This command encapsulates a set of sqlite statement, which are executed
// within a transaction.
type TransactionExecuteCommandSet struct {
	Stmts []string `json:"stmts"`
}

// Creates a new set of sqlite commands, which execute within a transaction.
func NewTransactionExecuteCommandSet(stmts []string) *TransactionExecuteCommandSet {
	return &TransactionExecuteCommandSet{
		Stmts: stmts,
	}
}

// The name of the command in the log.
func (c *TransactionExecuteCommandSet) CommandName() string {
	return "transaction_execute"
}

// Executes a set of sqlite statements, within a transaction. All statements
// will take effect, or none.
func (c *TransactionExecuteCommandSet) Apply(server raft.Server) (interface{}, error) {
	log.Trace("Applying TransactionExecuteCommandSet of size %d", len(c.Stmts))

	commitSuccess := false
	db := server.Context().(*db.DB)
	defer func() {
		if !commitSuccess {
			err := db.RollbackTransaction()
			if err != nil {
				log.Error("Failed to rollback transaction: %s", err.Error)
			}
		}
	}()

	err := db.StartTransaction()
	if err != nil {
		log.Error("Failed to start transaction:", err.Error())
		return nil, err
	}
	for i := range c.Stmts {
		err = db.Execute(c.Stmts[i])
		if err != nil {
			log.Error("Failed to execute statement within transaction", err.Error())
			return nil, err
		}
	}
	err = db.CommitTransaction()
	if err != nil {
		log.Error("Failed to commit transaction:", err.Error())
		return nil, err
	} else {
		commitSuccess = true
	}
	return nil, nil
}
