package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/mkideal/pkg/textutil"
	cl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

// Result represents execute result. It is possible that an execute result
// returns Rows (for example, if RETURNING clause is used in an INSERT statement).
type Result struct {
	TableData
	LastInsertID int     `json:"last_insert_id,omitempty"`
	RowsAffected int     `json:"rows_affected,omitempty"`
	Time         float64 `json:"time,omitempty"`
	Error        string  `json:"error,omitempty"`
}

// unifiedResponse represents the response structure from the /db/request endpoint
type unifiedResponse struct {
	Results []unifiedResult `json:"results"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

// unifiedResult represents a single result item that can be either query or execute
type unifiedResult struct {
	TableData

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
		TableData: ur.TableData,
		Time:      ur.Time,
		Error:     ur.Error,
	}
}

// toResult converts a unified result to Result format for display
func (ur *unifiedResult) toResult() *Result {
	return &Result{
		TableData:    ur.TableData,
		LastInsertID: ur.LastInsertID,
		RowsAffected: ur.RowsAffected,
		Time:         ur.Time,
		Error:        ur.Error,
	}
}

// requestWithClient sends SQL statements to the unified /db/request endpoint
// and handles the response appropriately based on the result type
func requestWithClient(output io.Writer, client *cl.Client, timer, forceWrites, changes bool, stmt string) error {
	queryStr := url.Values{}
	if timer {
		queryStr.Set("timings", "")
	}

	bP := "db/request"
	if forceWrites {
		bP = "db/execute"
	}
	p, err := url.JoinPath(client.Prefix, bP)
	if err != nil {
		return err
	}
	u := &url.URL{
		Path:     p,
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
	if err := parseResponse(response, &ret); err != nil {
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
		fmt.Fprintf(output, "Error: %s\n", result.Error)
		return hcr
	}

	// Determine how to display the result based on its content
	if result.isQueryResult() {
		// Display as query result
		rows := result.toRows()
		if err := rows.validate(); err != nil {
			return err
		}
		textutil.WriteTable(output, rows, headerRender)
		if timer {
			fmt.Fprintf(output, "Run Time: %f seconds\n", result.Time)
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
				fmt.Fprintf(output, "%d %s affected (%f sec)\n", executeResult.RowsAffected, rowString, executeResult.Time)
			} else {
				fmt.Fprintf(output, "%d %s affected\n", executeResult.RowsAffected, rowString)
			}
			if changes {
				fmt.Fprintf(output, "last insert ID: %d\n", executeResult.LastInsertID)
			}
		} else {
			textutil.WriteTable(output, executeResult, headerRender)
		}
	}

	return hcr
}
