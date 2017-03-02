package store

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	jsonPeerPath = "peers.json"
)

// NumPeers returns the number of peers indicated by the config files
// within raftDir.
//
// This code makes assumptions about how the Raft module works.
func NumPeers(raftDir string) (int, error) {
	// Read the file
	buf, err := ioutil.ReadFile(filepath.Join(raftDir, jsonPeerPath))
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}

	// Check for no peers
	if len(buf) == 0 {
		return 0, nil
	}

	// Decode the peers
	var peerSet []string
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&peerSet); err != nil {
		return 0, err
	}

	return len(peerSet), nil
}

// JoinAllowed returns whether the config files within raftDir indicate
// that the node can join a cluster.
func JoinAllowed(raftDir string) (bool, error) {
	n, err := NumPeers(raftDir)
	if err != nil {
		return false, err
	}
	return n <= 1, nil
}
