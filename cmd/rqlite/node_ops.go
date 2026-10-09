package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	httpcl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

// removeNode asks the cluster to remove the node with the given ID.
func removeNode(client *httpcl.Client, id string) error {
	return sendNodeID(client, "remove", client.Delete, id)
}

// demoteNode asks the cluster to demote the voting node with the given ID
// to a non-voter.
func demoteNode(client *httpcl.Client, id string) error {
	return sendNodeID(client, "demote", client.Post, id)
}

// sendNodeID sends a JSON body of the form {"id": "<id>"} to the given
// endpoint, using the given request function.
func sendNodeID(client *httpcl.Client, endpoint string, send func(string, io.Reader) (*http.Response, error), id string) error {
	b, err := json.Marshal(map[string]string{
		"id": id,
	})
	if err != nil {
		return err
	}

	u := fmt.Sprintf("%s%s", client.Prefix, endpoint)
	resp, err := send(u, bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with: %s", resp.Status)
	}

	return nil
}
