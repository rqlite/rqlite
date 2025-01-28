package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	httpcl "github.com/rqlite/rqlite/v8/cmd/rqlite/http"
)

func removeNode(client *httpcl.Client, id string, argv *argT) error {
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   address6(argv),
		Path:   fmt.Sprintf("%sremove", argv.Prefix),
	}
	urlStr := u.String()

	b, err := json.Marshal(map[string]string{
		"id": id,
	})
	if err != nil {
		return err
	}

	nRedirect := 0
	for {
		resp, err := client.Delete(urlStr, bytes.NewReader(b))
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("unauthorized")
		}

		if resp.StatusCode == http.StatusMovedPermanently {
			nRedirect++
			if nRedirect > maxRedirect {
				return fmt.Errorf("maximum leader redirect limit exceeded")
			}
			urlStr = resp.Header["Location"][0]
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("server responded with: %s", resp.Status)
		}

		return nil
	}
}
