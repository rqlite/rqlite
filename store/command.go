package store

import (
	"encoding/json"
)

// commandType are commands that affect the state of the cluster,
// and must go through Raft.
type commandType int

const (
	execute     commandType = iota // Commands which modify the database.
	query                          // Commands which query the database.
	metadataSet                    // Commands that set Store metadata.
	metadataGet                    // Commands that get store metadata.
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

// databaseSub is a command sub which involves interaction with the database.
type databaseSub struct {
	Tx      bool     `json:"tx,omitempty"`
	Queries []string `json:"queries,omitempty"`
	Timings bool     `json:"timings,omitempty"`
}

// metadataSetSub is a command that sets a key-value pair.
type metadataSetSub struct {
	NodeID string
	Key    string
	Value  string
}
