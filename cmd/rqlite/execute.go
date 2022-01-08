package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/mkideal/cli"
	cl "github.com/rqlite/rqlite/http"
)

// Result represents execute result
type Result struct {
	LastInsertID int     `json:"last_insert_id,omitempty"`
	RowsAffected int     `json:"rows_affected,omitempty"`
	Time         float64 `json:"time,omitempty"`
	Error        string  `json:"error,omitempty"`
}

type executeResponse struct {
	Results []*Result `json:"results,omitempty"`
	Error   string    `json:"error,omitempty"`
	Time    float64   `json:"time,omitempty"`
}

func executeWithClient(ctx *cli.Context, client *http.Client, argv *argT, timer bool, stmt string) error {
	queryStr := url.Values{}
	if timer {
		queryStr.Set("timings", "")
	}
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:   fmt.Sprintf("%sdb/execute", argv.Prefix),
	}

	requestData := strings.NewReader(makeJSONBody(stmt))

	hosts := make([]string, 0)

	if argv.Hosts == "" {
		// fallback to the Host:Port pair
		hosts = append(hosts, fmt.Sprintf("%s:%d", argv.Host, argv.Port))
	} else {
		hosts = append(hosts, strings.Split(argv.Hosts, ",")...)
	}

	recoveringClient := cl.NewClient(client, hosts, cl.WithScheme(argv.Protocol), cl.WithBasicAuth(argv.Credentials))

	if _, err := requestData.Seek(0, io.SeekStart); err != nil {
		return err
	}

	resp, err := recoveringClient.Execute(u, requestData)

	if err != nil {
		return err
	}
	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("unauthorized")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with %s: %s", resp.Status, response)
	}

	// Parse response and write results
	ret := &executeResponse{}
	if err := parseResponse(&response, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return fmt.Errorf(ret.Error)
	}
	if len(ret.Results) != 1 {
		return fmt.Errorf("unexpected results length: %d", len(ret.Results))
	}

	result := ret.Results[0]
	if result.Error != "" {
		ctx.String("Error: %s\n", result.Error)
		return nil
	}

	rowString := "row"
	if result.RowsAffected > 1 {
		rowString = "rows"
	}
	if timer {
		ctx.String("%d %s affected (%f sec)\n", result.RowsAffected, rowString, result.Time)
	} else {
		ctx.String("%d %s affected\n", result.RowsAffected, rowString)
	}

	if timer {
		fmt.Printf("Run Time: %f seconds\n", result.Time)
	}
	return nil
}
