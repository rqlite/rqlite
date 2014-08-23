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
	db.Exec(c.Stmt)
	return nil, nil
}
