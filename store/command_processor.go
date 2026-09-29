package store

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/rqlite/rqlite/v10/command"
	"github.com/rqlite/rqlite/v10/command/chunking"
	"github.com/rqlite/rqlite/v10/command/proto"
	sql "github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

// ExecuteQueryResponses is a slice of ExecuteQueryResponse, which detects mutations.
type ExecuteQueryResponses []*proto.ExecuteQueryResponse

// Mutation returns true if any of the responses reports a successful write.
func (e ExecuteQueryResponses) Mutation() bool {
	if len(e) == 0 {
		return false
	}
	for i := range e {
		if e[i].GetMutated() {
			return true
		}
	}
	return false
}

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
// A non-nil error means the command could not safely be applied and the caller
// must stop applying commands. The other return values must not be used in that
// case. Ordinary SQL and operation errors are carried in the response.
func (c *CommandProcessor) Process(data []byte, db *sql.SwappableDB) (*proto.Command, bool, any, error) {
	cmd := &proto.Command{}
	if err := command.Unmarshal(data, cmd); err != nil {
		return nil, false, nil, fmt.Errorf("failed to unmarshal cluster command: %w", err)
	}

	switch cmd.Type {
	case proto.Command_COMMAND_TYPE_QUERY:
		var qr proto.QueryRequest
		if err := command.UnmarshalSubCommand(cmd, &qr); err != nil {
			return cmd, false, nil, fmt.Errorf("failed to unmarshal query subcommand: %w", err)
		}
		r, err := db.Query(qr.Request, qr.Timings)
		return cmd, false, &fsmQueryResponse{rows: r, error: err}, nil
	case proto.Command_COMMAND_TYPE_EXECUTE:
		var er proto.ExecuteRequest
		if err := command.UnmarshalSubCommand(cmd, &er); err != nil {
			return cmd, false, nil, fmt.Errorf("failed to unmarshal execute subcommand: %w", err)
		}
		r, err := db.Execute(er.Request, er.Timings)
		if _, ok := err.(*sql.FatalError); ok {
			return cmd, false, nil, err
		}
		return cmd, true, &fsmExecuteQueryResponse{results: r, error: err}, nil
	case proto.Command_COMMAND_TYPE_EXECUTE_QUERY:
		var eqr proto.ExecuteQueryRequest
		if err := command.UnmarshalSubCommand(cmd, &eqr); err != nil {
			return cmd, false, nil, fmt.Errorf("failed to unmarshal execute-query subcommand: %w", err)
		}
		r, err := db.Request(eqr.Request, eqr.Timings)
		if _, ok := err.(*sql.FatalError); ok {
			return cmd, false, nil, err
		}
		return cmd, ExecuteQueryResponses(r).Mutation(), &fsmExecuteQueryResponse{results: r, error: err}, nil
	case proto.Command_COMMAND_TYPE_LOAD:
		var lr proto.LoadRequest
		if err := command.UnmarshalLoadRequest(cmd.SubCommand, &lr); err != nil {
			return cmd, false, nil, fmt.Errorf("failed to unmarshal load subcommand: %w", err)
		}

		// create a scratch file in the same directory as s.db.Path()
		fd, err := createTemp(filepath.Dir(db.Path()), "rqlite-load-")
		if err != nil {
			return cmd, false, &fsmGenericResponse{error: fmt.Errorf("failed to create temporary database file: %s", err)}, nil
		}
		defer fsutil.Remove(fd.Name())
		defer fd.Close()
		_, err = fd.Write(lr.Data)
		if err != nil {
			return cmd, false, &fsmGenericResponse{error: fmt.Errorf("failed to write to temporary database file: %s", err)}, nil
		}
		fd.Close()

		// Swap the underlying database to the new one.
		if err := db.Swap(fd.Name(), db.FKEnabled(), db.WALEnabled()); err != nil {
			return cmd, false, &fsmGenericResponse{error: fmt.Errorf("error swapping databases: %s", err)}, nil
		}
		return cmd, true, &fsmGenericResponse{}, nil
	case proto.Command_COMMAND_TYPE_LOAD_CHUNK:
		var lcr proto.LoadChunkRequest
		if err := command.UnmarshalLoadChunkRequest(cmd.SubCommand, &lcr); err != nil {
			return cmd, false, nil, fmt.Errorf("failed to unmarshal load-chunk subcommand: %w", err)
		}

		dec, err := c.decMgmr.Get(lcr.StreamId)
		if err != nil {
			return cmd, false, &fsmGenericResponse{error: fmt.Errorf("failed to get dechunker: %s", err)}, nil
		}
		if lcr.Abort {
			path, err := dec.Close()
			if err != nil {
				return cmd, false, &fsmGenericResponse{error: fmt.Errorf("failed to close dechunker: %s", err)}, nil
			}
			c.decMgmr.Delete(lcr.StreamId)
			defer fsutil.Remove(path)
		} else {
			last, err := dec.WriteChunk(&lcr)
			if err != nil {
				return cmd, false, &fsmGenericResponse{error: fmt.Errorf("failed to write chunk: %s", err)}, nil
			}
			if last {
				path, err := dec.Close()
				if err != nil {
					return cmd, false, &fsmGenericResponse{error: fmt.Errorf("failed to close dechunker: %s", err)}, nil
				}
				c.decMgmr.Delete(lcr.StreamId)
				defer fsutil.Remove(path)

				// Check if reassembled database is valid. If not, do not perform the load. This could
				// happen a snapshot truncated earlier parts of the log which contained the earlier parts
				// of a database load. If that happened then the database has already been loaded, and
				// this load should be ignored.
				if !sql.IsValidSQLiteFile(path) {
					c.logger.Printf("invalid chunked database file - ignoring")
					return cmd, false, &fsmGenericResponse{error: fmt.Errorf("invalid chunked database file - ignoring")}, nil
				}
				if err := db.Swap(path, db.FKEnabled(), db.WALEnabled()); err != nil {
					return cmd, false, &fsmGenericResponse{error: fmt.Errorf("error swapping databases: %s", err)}, nil
				}
			}
		}
		return cmd, true, &fsmGenericResponse{}, nil
	case proto.Command_COMMAND_TYPE_NOOP:
		return cmd, false, &fsmGenericResponse{}, nil
	default:
		return cmd, false, &fsmGenericResponse{error: fmt.Errorf("unhandled command: %v", cmd.Type)}, nil
	}
}
