package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	httpcl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

func removeNode(client *httpcl.Client, id string) error {
	b, err := json.Marshal(map[string]string{
		"id": id,
	})
	if err != nil {
		return err
	}

	u := fmt.Sprintf("%sremove", client.Prefix)
	resp, err := client.Delete(u, bytes.NewReader(b))
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
