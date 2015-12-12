package command

import (
	"github.com/otoolep/raft"
	"github.com/jsrCorp/rqlite/db"

	"github.com/jsrCorp/rqlite/log"
)

// ExecuteCommand encapsulates a sqlite statement.
type ExecuteCommand struct {
	Stmt string `json:"stmt"`
}

// NewExecuteCommand creates a new Execute command.
func NewExecuteCommand(stmt string) *ExecuteCommand {
	return &ExecuteCommand{
		Stmt: stmt,
	}
}

// CommandName of the ExecuteCommand in the log.
func (c *ExecuteCommand) CommandName() string {
	return "execute"
}

// Apply executes an sqlite statement.
func (c *ExecuteCommand) Apply(server raft.Server) (interface{}, error) {
	log.Tracef("Applying ExecuteCommand: '%s'", c.Stmt)
	db := server.Context().(*db.DB)
	return nil, db.Execute(c.Stmt)
}

// TransactionExecuteCommandSet encapsulates a set of sqlite statement, which are executed
// within a transaction.
type TransactionExecuteCommandSet struct {
	Stmts []string `json:"stmts"`
}

// NewTransactionExecuteCommandSet Creates a new set of sqlite commands, which
// execute within a transaction.
func NewTransactionExecuteCommandSet(stmts []string) *TransactionExecuteCommandSet {
	return &TransactionExecuteCommandSet{
		Stmts: stmts,
	}
}

// CommandName of the TransactionExecute command in the log.
func (c *TransactionExecuteCommandSet) CommandName() string {
	return "transaction_execute"
}

// Apply executes a set of sqlite statements, within a transaction. All statements
// will take effect, or none.
func (c *TransactionExecuteCommandSet) Apply(server raft.Server) (interface{}, error) {
	log.Tracef("Applying TransactionExecuteCommandSet of size %d", len(c.Stmts))

	commitSuccess := false
	db := server.Context().(*db.DB)
	defer func() {
		if !commitSuccess {
			err := db.RollbackTransaction()
			if err != nil {
				log.Errorf("Failed to rollback transaction: %s", err.Error())
			}
		}
	}()

	err := db.StartTransaction()
	if err != nil {
		log.Errorf("Failed to start transaction: %s", err.Error())
		return nil, err
	}

	for i := range c.Stmts {
		err = db.Execute(c.Stmts[i])
		if err != nil {
			log.Errorf("Failed to execute statement within transaction: %s", err.Error())
			return nil, err
		}
	}

	if err = db.CommitTransaction(); err != nil {
		log.Errorf("Failed to commit transaction: %s", err.Error())
		return nil, err
	}

	commitSuccess = true
	return nil, nil
}
