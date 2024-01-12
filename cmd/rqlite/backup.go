package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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
		Host:     address6(argv),
		Path:     fmt.Sprintf("%sdb/backup", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = sendRequestW(ctx, makeBackupRequest, u.String(), argv, fd)
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
		Host:     address6(argv),
		Path:     fmt.Sprintf("%sdb/backup", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = sendRequestW(ctx, makeBackupRequest, u.String(), argv, fd)
	if err != nil {
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

func makeRestoreRequest(b []byte) func(string) (*http.Request, error) {
	header := "text/plain"
	if validSQLiteData(b) {
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
	statusRet, err := checkStatus(ctx, argv)
	if err != nil {
		return err
	}

	restoreFile, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	if !validSQLiteData(restoreFile) {
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
		Host:     address6(argv),
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

func boot(ctx *cli.Context, filename string, argv *argT) error {
	if _, err := checkStatus(ctx, argv); err != nil {
		return err
	}

	client := http.Client{Transport: &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure},
	}}

	// File is OK?
	if !validSQLiteFile(filename) {
		return fmt.Errorf("%s is not a valid SQLite file", filename)
	}

	// Do the boot.
	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	bootURL := fmt.Sprintf("%s://%s/boot", argv.Protocol, address6(argv))
	req, err := http.NewRequest("POST", bootURL, fd)
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

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("boot failed, status code: %s", resp.Status)
		if len(body) > 0 {
			errMsg += fmt.Sprintf(", %s", string(body))
		}
		return fmt.Errorf(errMsg)
	}

	ctx.String("Node booted successfully.\n")
	return nil
}

func checkStatus(ctx *cli.Context, argv *argT) (*statusResponse, error) {
	statusURL := fmt.Sprintf("%s://%s/status", argv.Protocol, address6(argv))
	client := http.Client{Transport: &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: argv.Insecure},
	}}

	req, err := http.NewRequest("GET", statusURL, nil)
	if err != nil {
		return nil, err
	}
	if argv.Credentials != "" {
		creds := strings.Split(argv.Credentials, ":")
		if len(creds) != 2 {
			return nil, fmt.Errorf("invalid Basic Auth credentials format")
		}
		req.SetBasicAuth(creds[0], creds[1])
	}

	statusResp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("unauthorized")
	}

	body, err := io.ReadAll(statusResp.Body)
	if err != nil {
		return nil, err
	}
	statusRet := &statusResponse{}
	if err := parseResponse(&body, &statusRet); err != nil {
		return nil, err
	}
	if statusRet.Store == nil {
		return nil, fmt.Errorf("unexpected server response: store status not found")
	}

	return statusRet, nil
}
