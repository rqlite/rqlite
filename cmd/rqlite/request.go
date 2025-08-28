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

// unifiedResponse represents the response structure from the /db/request endpoint
type unifiedResponse struct {
	Results []unifiedResult `json:"results"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

// unifiedResult represents a single result item that can be either query or execute
type unifiedResult struct {
	// Query result fields
	Columns []string `json:"columns,omitempty"`
	Types   []string `json:"types,omitempty"`
	Values  [][]any  `json:"values,omitempty"`

	// Execute result fields
	LastInsertID int `json:"last_insert_id,omitempty"`
	RowsAffected int `json:"rows_affected,omitempty"`

	// Common fields
	Error string  `json:"error,omitempty"`
	Time  float64 `json:"time,omitempty"`
}

// isQueryResult determines if this result contains query data (vs execute data)
func (ur *unifiedResult) isQueryResult() bool {
	return ur.Columns != nil || ur.Types != nil || ur.Values != nil
}

// toRows converts a unified result to Rows format for display
func (ur *unifiedResult) toRows() *Rows {
	return &Rows{
		Columns: ur.Columns,
		Types:   ur.Types,
		Values:  ur.Values,
		Time:    ur.Time,
		Error:   ur.Error,
	}
}

// toResult converts a unified result to Result format for display
func (ur *unifiedResult) toResult() *Result {
	return &Result{
		LastInsertID: ur.LastInsertID,
		RowsAffected: ur.RowsAffected,
		Time:         ur.Time,
		Error:        ur.Error,
	}
}

// requestWithClient sends SQL statements to the unified /db/request endpoint
// and handles the response appropriately based on the result type
func requestWithClient(ctx *cli.Context, client *cl.Client, timer bool, stmt string) error {
	queryStr := url.Values{}
	if timer {
		queryStr.Set("timings", "")
	}
	u := &url.URL{
		Path:     fmt.Sprintf("%sdb/request", client.Prefix),
		RawQuery: queryStr.Encode(),
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

	// Parse unified response
	ret := &unifiedResponse{}
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
		return hcr
	}

	// Determine how to display the result based on its content
	if result.isQueryResult() {
		// Display as query result
		rows := result.toRows()
		if err := rows.validate(); err != nil {
			return err
		}
		textutil.WriteTable(ctx, rows, headerRender)
		if timer {
			fmt.Printf("Run Time: %f seconds\n", result.Time)
		}
	} else {
		// Display as execute result
		executeResult := result.toResult()
		if executeResult.Columns == nil {
			rowString := "row"
			if executeResult.RowsAffected > 1 {
				rowString = "rows"
			}
			if timer {
				ctx.String("%d %s affected (%f sec)\n", executeResult.RowsAffected, rowString, executeResult.Time)
			} else {
				ctx.String("%d %s affected\n", executeResult.RowsAffected, rowString)
			}
		} else {
			textutil.WriteTable(ctx, executeResult, headerRender)
		}
	}

	return hcr
}
