package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/mkideal/pkg/textutil"
	cl "github.com/rqlite/rqlite/v10/cmd/rqlite/http"
)

// TableData holds tabular result data and implements the textutil.Table interface.
type TableData struct {
	Columns []string `json:"columns,omitempty"`
	Types   []string `json:"types,omitempty"`
	Values  [][]any  `json:"values,omitempty"`
}

func (t *TableData) RowCount() int {
	return len(t.Values) + 1
}

func (t *TableData) ColCount() int {
	return len(t.Columns)
}

func (t *TableData) Get(i, j int) string {
	if i == 0 {
		if j >= len(t.Columns) {
			return ""
		}
		return t.Columns[j]
	}

	if t.Values == nil {
		return "NULL"
	}

	if i-1 >= len(t.Values) {
		return "NULL"
	}
	if j >= len(t.Values[i-1]) {
		return "NULL"
	}
	return fmt.Sprintf("%v", t.Values[i-1][j])
}

// Rows represents query result
type Rows struct {
	TableData
	Time  float64 `json:"time"`
	Error string  `json:"error,omitempty"`
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

func queryWithClient(output io.Writer, client *cl.Client, timer, blobArray bool, consistency, query string) error {
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
		var innerErr *cl.HostChangedError
		ok := errors.As(err, &innerErr)
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
	if err := result.validate(); err != nil {
		return err
	}
	textutil.WriteTable(output, result, headerRender)

	if timer {
		fmt.Fprintf(output, "Run Time: %f seconds\n", result.Time)
	}
	return hcr
}
