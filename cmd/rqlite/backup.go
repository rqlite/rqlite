package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/mkideal/cli"
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

func makeBackupRequest(urlStr string) (*http.Request, error) {
	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func backup(ctx *cli.Context, filename string, argv *argT) error {
	queryStr := url.Values{}
	u := url.URL{
		Scheme:   argv.Protocol,
		Host:     fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:     fmt.Sprintf("%sdb/backup", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := sendRequest(ctx, makeBackupRequest, u.String(), argv)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filename, *response, 0644)
	if err != nil {
		return err
	}

	ctx.String("backup file written successfully\n")
	return nil
}

func dump(ctx *cli.Context, filename string, argv *argT) error {
	queryStr := url.Values{}
	queryStr.Set("fmt", "sql")
	u := url.URL{
		Scheme:   argv.Protocol,
		Host:     fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:     fmt.Sprintf("%sdb/backup", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := sendRequest(ctx, makeBackupRequest, u.String(), argv)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filename, *response, 0644)
	if err != nil {
		return err
	}

	ctx.String("SQL text file written successfully\n")
	return nil
}

func validSQLiteFile(b []byte) bool {
	return len(b) > 13 && string(b[0:13]) == "SQLite format"
}

func makeRestoreRequest(b []byte) func(string) (*http.Request, error) {
	header := "text/plain"
	if validSQLiteFile(b) {
		header = "application/octet-stream"
	}
	return func(urlStr string) (*http.Request, error) {
		req, err := http.NewRequest("POST", urlStr, bytes.NewReader(b))
		req.Header.Set("Content-Type", header)
		if err != nil {
			return nil, err
		}
		return req, nil
	}
}

func restore(ctx *cli.Context, filename string, argv *argT) error {
	statusURL := fmt.Sprintf("%s://%s:%d/status", argv.Protocol, argv.Host, argv.Port)
	client := http.Client{Transport: &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure},
	}}

	req, err := http.NewRequest("GET", statusURL, nil)
	if err != nil {
		return err
	}
	if argv.Credentials != "" {
		creds := strings.Split(argv.Credentials, ":")
		if len(creds) != 2 {
			return fmt.Errorf("invalid Basic Auth credentials format")
		}
		req.SetBasicAuth(creds[0], creds[1])
	}

	statusResp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer statusResp.Body.Close()

	if statusResp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}

	body, err := ioutil.ReadAll(statusResp.Body)
	if err != nil {
		return err
	}
	statusRet := &statusResponse{}
	if err := parseResponse(&body, &statusRet); err != nil {
		return err
	}
	if statusRet.Store == nil {
		return fmt.Errorf("unexpected server response: store status not found")
	}

	restoreFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	if !validSQLiteFile(restoreFile) {
		// It is cheaper to append the actual pragma command to the restore file
		fkEnabled := statusRet.Store.SqliteStatus.FkConstraint == "enabled"
		if fkEnabled {
			restoreFile = append(restoreFile, []byte("PRAGMA foreign_keys=ON;")...)
		} else {
			restoreFile = append(restoreFile, []byte("PRAGMA foreign_keys=OFF;")...)
		}
	}

	queryStr := url.Values{}
	restoreURL := url.URL{
		Scheme:   argv.Protocol,
		Host:     fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:     fmt.Sprintf("%sdb/load", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := sendRequest(ctx, makeRestoreRequest(restoreFile), restoreURL.String(), argv)
	if err != nil {
		return err
	}

	restoreRet := &restoreResponse{}
	if err := parseResponse(response, &restoreRet); err != nil {
		return err
	}
	if !validSQLiteFile(restoreFile) {
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

	ctx.String("database restored successfully\n")
	return nil
}
