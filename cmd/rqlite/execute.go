package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/mkideal/cli"
	"github.com/mkideal/pkg/textutil"
	cl "github.com/rqlite/rqlite/v8/cmd/rqlite/http"
)

// Result represents execute result. It is possible that an execute result
// returns Rows (for example, if RETURNING clause is used in an INSERT statement).
type Result struct {
	LastInsertID int      `json:"last_insert_id,omitempty"`
	RowsAffected int      `json:"rows_affected,omitempty"`
	Columns      []string `json:"columns,omitempty"`
	Types        []string `json:"types,omitempty"`
	Values       [][]any  `json:"values,omitempty"`
	Time         float64  `json:"time,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// RowCount implements textutil.Table interface
func (r *Result) RowCount() int {
	return len(r.Values) + 1
}

// ColCount implements textutil.Table interface
func (r *Result) ColCount() int {
	return len(r.Columns)
}

// Get implements textutil.Table interface
func (r *Result) Get(i, j int) string {
	if i == 0 {
		if j >= len(r.Columns) {
			return ""
		}
		return r.Columns[j]
	}

	if r.Values == nil {
		return "NULL"
	}

	if i-1 >= len(r.Values) {
		return "NULL"
	}
	if j >= len(r.Values[i-1]) {
		return "NULL"
	}
	return fmt.Sprintf("%v", r.Values[i-1][j])
}

type executeResponse struct {
	Results []*Result `json:"results"`
	Error   string    `json:"error,omitempty"`
	Time    float64   `json:"time,omitempty"`
}

func executeWithClient(ctx *cli.Context, client *cl.Client, timer bool, stmt string) error {
	queryStr := url.Values{}
	if timer {
		queryStr.Set("timings", "")
	}
	u := &url.URL{
		Path: fmt.Sprintf("%sdb/execute", client.Prefix),
	}

	requestData := strings.NewReader(makeJSONBody(stmt))

	if _, err := requestData.Seek(0, io.SeekStart); err != nil {
		return err
	}

	resp, err := client.Execute(u, requestData)

	var hcr error
	if err != nil {
		// If the error is HostChangedError, it should be propagated back to the caller to handle
		// accordingly (change prompt display), but we should still assume that the request succeeded on some
		// host and not treat it as an error.
		err, ok := err.(*cl.HostChangedError)
		if !ok {
			return err
		}
		hcr = err
	}

	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with %s: %s", resp.Status, response)
	}

	// Parse response and write results
	ret := &executeResponse{}
	if err := parseResponse(&response, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	if len(ret.Results) != 1 {
		return fmt.Errorf("unexpected results length: %d", len(ret.Results))
	}

	result := ret.Results[0]
	if result.Error != "" {
		ctx.String("Error: %s\n", result.Error)
		return nil
	}

	if result.Columns == nil {
		rowString := "row"
		if result.RowsAffected > 1 {
			rowString = "rows"
		}
		if timer {
			ctx.String("%d %s affected (%f sec)\n", result.RowsAffected, rowString, result.Time)
			fmt.Printf("Run Time: %f seconds\n", result.Time) // Move this line inside the if timer block
		} else {
			ctx.String("%d %s affected\n", result.RowsAffected, rowString)
		}
	} else {
		textutil.WriteTable(ctx, result, headerRender)
	}
	return hcr
}
