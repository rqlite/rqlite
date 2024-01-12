package store

import (
	"fmt"
	"log"
	"os"

	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/chunking"
	"github.com/rqlite/rqlite/v8/command/proto"
	sql "github.com/rqlite/rqlite/v8/db"
)

// CommandProcessor processes commands by applying them to the underlying database.
type CommandProcessor struct {
	logger  *log.Logger
	decMgmr *chunking.DechunkerManager
}

// NewCommandProcessor returns a new instance of CommandProcessor.
func NewCommandProcessor(logger *log.Logger, dm *chunking.DechunkerManager) *CommandProcessor {
	return &CommandProcessor{
		logger:  logger,
		decMgmr: dm}
}

// Process processes the given command against the given database.
func (c *CommandProcessor) Process(data []byte, pDB **sql.DB) (*proto.Command, interface{}) {
	db := *pDB
	cmd := &proto.Command{}
	if err := command.Unmarshal(data, cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
	}

	switch cmd.Type {
	case proto.Command_COMMAND_TYPE_QUERY:
		var qr proto.QueryRequest
		if err := command.UnmarshalSubCommand(cmd, &qr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		r, err := db.Query(qr.Request, qr.Timings)
		return cmd, &fsmQueryResponse{rows: r, error: err}
	case proto.Command_COMMAND_TYPE_EXECUTE:
		var er proto.ExecuteRequest
		if err := command.UnmarshalSubCommand(cmd, &er); err != nil {
			panic(fmt.Sprintf("failed to unmarshal execute subcommand: %s", err.Error()))
		}
		r, err := db.Execute(er.Request, er.Timings)
		return cmd, &fsmExecuteResponse{results: r, error: err}
	case proto.Command_COMMAND_TYPE_EXECUTE_QUERY:
		var eqr proto.ExecuteQueryRequest
		if err := command.UnmarshalSubCommand(cmd, &eqr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal execute-query subcommand: %s", err.Error()))
		}
		r, err := db.Request(eqr.Request, eqr.Timings)
		return cmd, &fsmExecuteQueryResponse{results: r, error: err}
	case proto.Command_COMMAND_TYPE_LOAD:
		var lr proto.LoadRequest
		if err := command.UnmarshalLoadRequest(cmd.SubCommand, &lr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal load subcommand: %s", err.Error()))
		}

		// Swap the underlying database to the new one.
		if err := db.Close(); err != nil {
			return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to close post-load database: %s", err)}
		}
		if err := sql.RemoveFiles(db.Path()); err != nil {
			return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to remove existing database files: %s", err)}
		}

		newDB, err := createOnDisk(lr.Data, db.Path(), db.FKEnabled(), db.WALEnabled())
		if err != nil {
			return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to create on-disk database: %s", err)}
		}

		*pDB = newDB
		return cmd, &fsmGenericResponse{}
	case proto.Command_COMMAND_TYPE_LOAD_CHUNK:
		var lcr proto.LoadChunkRequest
		if err := command.UnmarshalLoadChunkRequest(cmd.SubCommand, &lcr); err != nil {
			panic(fmt.Sprintf("failed to unmarshal load-chunk subcommand: %s", err.Error()))
		}

		dec, err := c.decMgmr.Get(lcr.StreamId)
		if err != nil {
			return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to get dechunker: %s", err)}
		}
		if lcr.Abort {
			path, err := dec.Close()
			if err != nil {
				return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to close dechunker: %s", err)}
			}
			c.decMgmr.Delete(lcr.StreamId)
			defer os.Remove(path)
		} else {
			last, err := dec.WriteChunk(&lcr)
			if err != nil {
				return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to write chunk: %s", err)}
			}
			if last {
				path, err := dec.Close()
				if err != nil {
					return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to close dechunker: %s", err)}
				}
				c.decMgmr.Delete(lcr.StreamId)
				defer os.Remove(path)

				// Check if reassembled dayabase is valid. If not, do not perform the load. This could
				// happen a snapshot truncated earlier parts of the log which contained the earlier parts
				// of a database load. If that happened then the database has already been loaded, and
				// this load should be ignored.
				if !sql.IsValidSQLiteFile(path) {
					c.logger.Printf("invalid chunked database file - ignoring")
					return cmd, &fsmGenericResponse{error: fmt.Errorf("invalid chunked database file - ignoring")}
				}

				// Close the underlying database before we overwrite it.
				if err := db.Close(); err != nil {
					return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to close post-load database: %s", err)}
				}
				if err := sql.RemoveFiles(db.Path()); err != nil {
					return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to remove existing database files: %s", err)}
				}

				if err := os.Rename(path, db.Path()); err != nil {
					return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to rename temporary database file: %s", err)}
				}
				newDB, err := sql.Open(db.Path(), db.FKEnabled(), db.WALEnabled())
				if err != nil {
					return cmd, &fsmGenericResponse{error: fmt.Errorf("failed to open new on-disk database: %s", err)}
				}

				// Swap the underlying database to the new one.
				*pDB = newDB
			}
		}
		return cmd, &fsmGenericResponse{}
	case proto.Command_COMMAND_TYPE_NOOP:
		return cmd, &fsmGenericResponse{}
	default:
		return cmd, &fsmGenericResponse{error: fmt.Errorf("unhandled command: %v", cmd.Type)}
	}
}
