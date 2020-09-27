package store

import (
	"encoding/json"
)

// commandType are commands that affect the state of the cluster, and must go through Raft.
type commandType int

const (
	execute        commandType = iota // Commands which modify the database.
	query                             // Commands which query the database.
	metadataSet                       // Commands which sets Store metadata
	metadataDelete                    // Commands which deletes Store metadata
)

type command struct {
	Typ commandType     `json:"typ,omitempty"`
	Sub json.RawMessage `json:"sub,omitempty"`
}

func newCommand(t commandType, d interface{}) (*command, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	return &command{
		Typ: t,
		Sub: b,
	}, nil
}

func newMetadataSetCommand(id string, md map[string]string) (*command, error) {
	m := metadataSetSub{
		RaftID: id,
		Data:   md,
	}
	return newCommand(metadataSet, m)
}

// databaseSub is a command sub which involves interaction with the database.
// Queries and Parameters are separate fields, for backwards-compatibility
// reasons. Unless Parameters is nil, it should be the same length as Queries.
type databaseSub struct {
	Tx         bool      `json:"tx,omitempty"`
	Queries    []string  `json:"queries,omitempty"`
	Parameters [][]Value `json:"Parameters,omitempty`
	Timings    bool      `json:"timings,omitempty"`
}

type metadataSetSub struct {
	RaftID string            `json:"raft_id,omitempty"`
	Data   map[string]string `json:"data,omitempty"`
}
