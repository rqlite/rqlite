package main

import (
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/mkideal/cli"
	cl "github.com/rqlite/rqlite/cmd/rqlite/http"
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

func backupWithClient(ctx *cli.Context, client *cl.Client, filename string) error {
	queryStr := url.Values{}
	u := url.URL{
		Path:     fmt.Sprintf("%sdb/backup", client.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := client.Backup(u)
	if err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, response.Body)
	if err != nil {
		return err
	}

	ctx.String("backup file written successfully\n")
	return nil
}

func dumpWithClient(ctx *cli.Context, client *cl.Client, filename string) error {
	queryStr := url.Values{}
	queryStr.Set("fmt", "sql")
	u := url.URL{
		Path:     fmt.Sprintf("%sdb/backup", client.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := client.Backup(u)
	if err != nil {
		return err
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, response.Body)
	if err != nil {
		return err
	}

	ctx.String("SQL text file written successfully\n")
	return nil
}

func restoreWithClient(ctx *cli.Context, client *cl.Client, filename string) error {
	restoreFile, err := os.Open(filename)
	if err != nil {
		return err
	}

	queryStr := url.Values{}
	restoreURL := url.URL{
		Path:     fmt.Sprintf("%sdb/load", client.Prefix),
		RawQuery: queryStr.Encode(),
	}
	resp, err := client.Restore(restoreURL, restoreFile)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	restoreRet := &restoreResponse{}
	if err := parseResponse(&response, &restoreRet); err != nil {
		return err
	}
	if len(restoreRet.Results) < 1 {
		return fmt.Errorf("unexpected results length: %d", len(restoreRet.Results))
	}
	if resultError := restoreRet.Results[0].Error; resultError != "" {
		ctx.String("Error: %s\n", resultError)
		return nil
	}

	ctx.String("last inserted ID: %d\n", restoreRet.Results[0].LastInsertID)
	ctx.String("rows affected: %d\n", restoreRet.Results[0].RowsAffected)
	ctx.String("database restored successfully\n")
	return nil
}
