package store

import (
	"encoding/json"
)

// commandType are commands that affect the state of the cluster, and must go through Raft.
type commandType int

const (
	execute commandType = iota // Commands which modify the database.
	query                      // Commands which query the database.
	peer                       // Commands that modify peers map.
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

// peersSub is a command which sets the API address for a Raft address.
type peersSub map[string]string
