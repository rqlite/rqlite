package legacy

import (
	"encoding/json"
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/rqlite/rqlite/command"
)

const (
	execute        commandType = iota // Commands which modify the database.
	query                             // Commands which query the database.
	metadataSet                       // Commands which sets Store metadata
	metadataDelete                    // Commands which deletes Store metadata
)

var (
	// ErrNotLegacyCommand  is returned when a command is not legacy encoded.
	ErrNotLegacyCommand = errors.New("not legacy command")

	// ErrUnknownCommandType is returned when an unknown command type is encountered.
	ErrUnknownCommandType = errors.New("unknown command type")

	// ErrUnsupportedType is returned when a request contains an unsupported type.
	ErrUnsupportedType = errors.New("unsupported type")
)

// commandType are commands that affect the state of the cluster, and must go through Raft.
type commandType int

// Value is the type for parameters passed to a parameterized SQL statement.
type Value interface{}

// Command is the type of legacy JSON-encoded commands in the Raft log.
type Command struct {
	Typ commandType     `json:"typ,omitempty"`
	Sub json.RawMessage `json:"sub,omitempty"`
}

// databaseSub is a command sub which involves interaction with the database.
// Queries and Parameters are separate fields, for backwards-compatibility
// reasons. Unless Parameters is nil, it should be the same length as Queries.
type databaseSub struct {
	Tx         bool      `json:"tx,omitempty"`
	SQLs       []string  `json:"queries,omitempty"`
	Parameters [][]Value `json:"Parameters,omitempty`
	Timings    bool      `json:"timings,omitempty"`
}

type metadataSetSub struct {
	RaftID string            `json:"raft_id,omitempty"`
	Data   map[string]string `json:"data,omitempty"`
}

// Unmarshal unmarshals a legacy JSON-encoded command in the Raft log.
func Unmarshal(b []byte, c *command.Command) error {
	if b == nil || len(b) == 0 || b[0] != '{' {
		return ErrNotLegacyCommand
	}

	var lc Command
	if err := json.Unmarshal(b, &lc); err != nil {
		return err
	}

	var m proto.Message
	switch lc.Typ {
	case execute, query:
		var d databaseSub
		if err := json.Unmarshal(lc.Sub, &d); err != nil {
			return err
		}

		stmts, err := subCommandToStatements(&d)
		if err != nil {
			return err
		}

		if lc.Typ == execute {
			c.Type = command.Command_COMMAND_TYPE_EXECUTE
			m = &command.ExecuteRequest{
				Request: &command.Request{
					Transaction: d.Tx,
					Statements:  stmts,
				},
				Timings: d.Timings,
			}
		} else {
			c.Type = command.Command_COMMAND_TYPE_QUERY
			m = &command.QueryRequest{
				Request: &command.Request{
					Transaction: d.Tx,
					Statements:  stmts,
				},
				Timings: d.Timings,
			}
		}

	case metadataSet:
		var d metadataSetSub
		if err := json.Unmarshal(lc.Sub, &d); err != nil {
			return err
		}

		c.Type = command.Command_COMMAND_TYPE_METADATA_SET
		m = &command.MetadataSet{
			RaftId: d.RaftID,
			Data:   d.Data,
		}
	case metadataDelete:
		var d string
		if err := json.Unmarshal(lc.Sub, &d); err != nil {
			return err
		}

		c.Type = command.Command_COMMAND_TYPE_METADATA_DELETE
		m = &command.MetadataDelete{
			RaftId: d,
		}
	default:
		return ErrUnknownCommandType
	}

	// Just marshal it, forget about compression, this will
	// never go to disk after all.
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	c.SubCommand = b
	c.Compressed = false

	return nil
}

func subCommandToStatements(d *databaseSub) ([]*command.Statement, error) {
	stmts := make([]*command.Statement, len(d.SQLs))

	for i := range d.SQLs {
		stmts[i] = &command.Statement{
			Sql:        d.SQLs[i],
			Parameters: nil,
		}

		// Support backwards-compatibility, since old versions didn't
		// have any Parameters in legacy Raft commands
		if len(d.Parameters) == 0 {
			continue
		}
		stmts[i].Parameters = make([]*command.Parameter, len(d.Parameters[i]))
		for j := range d.Parameters[i] {
			switch v := d.Parameters[i][j].(type) {
			case int:
			case int64:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_I{
						I: v,
					},
				}
			case float64:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_D{
						D: v,
					},
				}
			case bool:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_B{
						B: v,
					},
				}
			case []byte:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_Y{
						Y: v,
					},
				}
			case string:
				stmts[i].Parameters[j] = &command.Parameter{
					Value: &command.Parameter_S{
						S: v,
					},
				}
			default:
				return nil, ErrUnsupportedType
			}
		}
	}
	return stmts, nil
}
