package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/mkideal/cli"
	httpcl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

type restoreResponse struct {
	Results []*Result `json:"results"`
}

type sqliteStatus struct {
	FkConstraint string `json:"fk_constraints"`
}

type store struct {
	SqliteStatus sqliteStatus `json:"sqlite3"`
}

type statusResponse struct {
	Store *store `json:"store"`
}

func backup(ctx *cli.Context, client *httpcl.Client, filename string) error {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	u := fmt.Sprintf("%sdb/backup", client.Prefix)
	resp, err := client.Get(u)
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

	if _, err := io.Copy(fd, resp.Body); err != nil {
		return err
	}

	ctx.String("backup file written successfully\n")
	return nil
}

func dump(ctx *cli.Context, client *httpcl.Client, filename string, tables []string) error {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	queryStr := url.Values{}
	queryStr.Set("fmt", "sql")
	if len(tables) > 0 {
		queryStr.Set("tables", strings.Join(tables, ","))
	}
	u := fmt.Sprintf("%sdb/backup?%s", client.Prefix, queryStr.Encode())

	resp, err := client.Get(u)
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

	if _, err := io.Copy(fd, resp.Body); err != nil {
		return err
	}

	ctx.String("SQL text file written successfully\n")
	return nil
}

func validSQLiteFile(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	b := make([]byte, 16)
	_, err = file.Read(b)
	if err != nil {
		return false
	}
	return validSQLiteData(b)
}

func validSQLiteData(b []byte) bool {
	return len(b) > 13 && string(b[0:13]) == "SQLite format"
}

func restore(ctx *cli.Context, client *httpcl.Client, filename string) error {
	statusRet, err := checkStatus(client)
	if err != nil {
		return err
	}

	restoreFile, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	if !validSQLiteData(restoreFile) {
		fkEnabled := statusRet.Store.SqliteStatus.FkConstraint == "enabled"
		if fkEnabled {
			restoreFile = append(restoreFile, []byte("PRAGMA foreign_keys=ON;")...)
		} else {
			restoreFile = append(restoreFile, []byte("PRAGMA foreign_keys=OFF;")...)
		}
	}

	contentType := "text/plain"
	if validSQLiteData(restoreFile) {
		contentType = "application/octet-stream"
	}

	u := fmt.Sprintf("%sdb/load", client.Prefix)
	headers := http.Header{"Content-Type": {contentType}}
	resp, err := client.PostWithHeaders(u, bytes.NewReader(restoreFile), headers)
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

	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	restoreRet := &restoreResponse{}
	if err := parseResponse(response, &restoreRet); err != nil {
		return err
	}
	if !validSQLiteData(restoreFile) {
		if len(restoreRet.Results) < 1 {
			return fmt.Errorf("unexpected results length: %d", len(restoreRet.Results))
		}
		if resultError := restoreRet.Results[0].Error; resultError != "" {
			ctx.String("Error: %s\n", resultError)
			return nil
		}
		ctx.String("last inserted ID: %d\n", restoreRet.Results[0].LastInsertID)
		ctx.String("rows affected: %d\n", restoreRet.Results[0].RowsAffected)
	}

	ctx.String("Database restored successfully.\n")
	return nil
}

func boot(ctx *cli.Context, client *httpcl.Client, filename string) error {
	if _, err := checkStatus(client); err != nil {
		return err
	}

	if !validSQLiteFile(filename) {
		return fmt.Errorf("%s is not a valid SQLite file", filename)
	}

	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	u := fmt.Sprintf("%sboot", client.Prefix)
	resp, err := client.Post(u, fd)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("boot failed, status code: %s", resp.Status)
		if len(body) > 0 {
			errMsg += fmt.Sprintf(", %s", string(body))
		}
		return errors.New(errMsg)
	}

	ctx.String("Node booted successfully.\n")
	return nil
}

func checkStatus(client *httpcl.Client) (*statusResponse, error) {
	u := fmt.Sprintf("%sstatus", client.Prefix)
	resp, err := client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("unauthorized")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	statusRet := &statusResponse{}
	if err := parseResponse(body, &statusRet); err != nil {
		return nil, err
	}
	if statusRet.Store == nil {
		return nil, fmt.Errorf("unexpected server response: store status not found")
	}

	return statusRet, nil
}
