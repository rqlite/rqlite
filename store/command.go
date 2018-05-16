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
	connect                           // Commands which create a database connection
	disconnect                        // Commands which disconnect from the database.
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
type databaseSub struct {
	ConnID  uint64   `json:"conn_id,omitempty"`
	Tx      bool     `json:"tx,omitempty"`
	Queries []string `json:"queries,omitempty"`
	Timings bool     `json:"timings,omitempty"`
}

type metadataSetSub struct {
	RaftID string            `json:"raft_id,omitempty"`
	Data   map[string]string `json:"data,omitempty"`
}

type connectionSub struct {
	ConnID uint64 `json:"conn_id,omitempty"`
}
