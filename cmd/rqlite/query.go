package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/mkideal/cli"
	"github.com/mkideal/pkg/textutil"
	cl "github.com/rqlite/rqlite/v8/cmd/rqlite/http"
)

// Rows represents query result
type Rows struct {
	Columns []string `json:"columns"`
	Types   []string `json:"types"`
	Values  [][]any  `json:"values"`
	Time    float64  `json:"time"`
	Error   string   `json:"error,omitempty"`
}

// RowCount implements textutil.Table interface
func (r *Rows) RowCount() int {
	return len(r.Values) + 1
}

// ColCount implements textutil.Table interface
func (r *Rows) ColCount() int {
	return len(r.Columns)
}

// Get implements textutil.Table interface
func (r *Rows) Get(i, j int) string {
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

func (r *Rows) validate() error {
	if r.Error != "" {
		return errors.New(r.Error)
	}
	return nil
}

// headerRenderStyle render the header of result
type headerRenderStyle struct {
	textutil.DefaultStyle
}

func (render headerRenderStyle) CellRender(row, col int, cell string, cw *textutil.ColorWriter) {
	if row != 0 {
		fmt.Fprint(cw, cell)
	} else {
		fmt.Fprint(cw, cw.Color.Cyan(cell))
	}
}

var headerRender = &headerRenderStyle{}

type queryResponse struct {
	Results []*Rows `json:"results"`
	Error   string  `json:"error,omitempty"`
	Time    float64 `json:"time"`
}

func queryWithClient(ctx *cli.Context, client *cl.Client, timer, blobArray bool, consistency, query string) error {
	queryStr := url.Values{}
	queryStr.Set("level", consistency)
	queryStr.Set("q", query)
	if timer {
		queryStr.Set("timings", "")
	}
	if blobArray {
		queryStr.Set("blob_array", "")
	}
	u := &url.URL{
		Path:     fmt.Sprintf("%sdb/query", client.Prefix),
		RawQuery: queryStr.Encode(),
	}

	var hcr error
	resp, err := client.Query(u)
	if err != nil {
		// If the error is HostChangedError, it should be propagated back to the caller to handle
		// accordingly (change prompt display), but we should still assume that the request succeeded on some
		// host and not treat it as an error.
		innerErr, ok := err.(*cl.HostChangedError)
		if !ok {
			return err
		}
		hcr = innerErr
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
	ret := &queryResponse{}
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
	if err := result.validate(); err != nil {
		return err
	}
	textutil.WriteTable(ctx, result, headerRender)

	if timer {
		fmt.Printf("Run Time: %f seconds\n", result.Time)
	}
	return hcr
}
