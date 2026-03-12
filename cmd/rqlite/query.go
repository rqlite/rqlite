package main

import (
	"encoding/csv"
	"encoding/json"
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

// writeTable formats and writes TableData to the writer in the given mode.
func writeTable(w io.Writer, t *TableData, mode string) {
	switch mode {
	case "csv":
		writeCSV(w, t)
	case "json":
		writeJSON(w, t)
	case "line":
		writeLine(w, t)
	default:
		textutil.WriteTable(w, t, headerRender)
	}
}

func writeCSV(w io.Writer, t *TableData) {
	cw := csv.NewWriter(w)
	cw.Write(t.Columns)
	for _, row := range t.Values {
		record := make([]string, len(t.Columns))
		for j := range t.Columns {
			if j < len(row) && row[j] != nil {
				record[j] = fmt.Sprintf("%v", row[j])
			} else {
				record[j] = "NULL"
			}
		}
		cw.Write(record)
	}
	cw.Flush()
}

func writeJSON(w io.Writer, t *TableData) {
	rows := make([]map[string]any, 0, len(t.Values))
	for _, row := range t.Values {
		m := make(map[string]any, len(t.Columns))
		for j, col := range t.Columns {
			if j < len(row) {
				m[col] = row[j]
			} else {
				m[col] = nil
			}
		}
		rows = append(rows, m)
	}
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		fmt.Fprintf(w, "Error: %s\n", err)
		return
	}
	fmt.Fprintf(w, "%s\n", data)
}

func writeLine(w io.Writer, t *TableData) {
	maxWidth := 0
	for _, col := range t.Columns {
		if len(col) > maxWidth {
			maxWidth = len(col)
		}
	}
	for i, row := range t.Values {
		if i > 0 {
			fmt.Fprintln(w)
		}
		for j, col := range t.Columns {
			val := "NULL"
			if j < len(row) && row[j] != nil {
				val = fmt.Sprintf("%v", row[j])
			}
			fmt.Fprintf(w, "%*s = %s\n", maxWidth, col, val)
		}
	}
}

type queryResponse struct {
	Results []*Rows `json:"results"`
	Error   string  `json:"error,omitempty"`
	Time    float64 `json:"time"`
}

func queryWithClient(output io.Writer, client *cl.Client, timer, blobArray bool, consistency, mode, query string) error {
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
	writeTable(output, &result.TableData, mode)

	if timer {
		fmt.Fprintf(output, "Run Time: %f seconds\n", result.Time)
	}
	return hcr
}
