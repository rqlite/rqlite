package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/mkideal/cli"
	"github.com/mkideal/pkg/textutil"
)

// Rows represents query result
type Rows struct {
	Columns []string        `json:"columns"`
	Types   []string        `json:"types"`
	Values  [][]interface{} `json:"values"`
	Time    float64         `json:"time"`
	Error   string          `json:"error,omitempty"`
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
		return fmt.Errorf(r.Error)
	}
	return nil
}

// headerRenderStyle render the header of result
type headerRenderStyle struct {
	textutil.DefaultStyle
}

func (render headerRenderStyle) CellRender(row, col int, cell string, cw *textutil.ColorWriter) {
	if row != 0 {
		fmt.Fprintf(cw, cell)
	} else {
		fmt.Fprintf(cw, cw.Color.Cyan(cell))
	}
}

var headerRender = &headerRenderStyle{}

type queryResponse struct {
	Results []*Rows `json:"results"`
	Error   string  `json:"error,omitempty"`
	Time    float64 `json:"time"`
}

func queryWithClient(ctx *cli.Context, client *http.Client, argv *argT, timer bool, consistency, query string) error {
	queryStr := url.Values{}
	queryStr.Set("level", consistency)
	queryStr.Set("q", query)
	if timer {
		queryStr.Set("timings", "")
	}
	u := url.URL{
		Scheme:   argv.Protocol,
		Host:     fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:     fmt.Sprintf("%sdb/query", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}
	urlStr := u.String()

	nRedirect := 0
	for {
		req, err := http.NewRequest("GET", urlStr, nil)
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
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("unauthorized")
		}

		if resp.StatusCode == http.StatusMovedPermanently {
			nRedirect++
			if nRedirect > maxRedirect {
				return fmt.Errorf("maximum leader redirect limit exceeded")
			}
			urlStr = resp.Header["Location"][0]
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("server responded with %s: %s", resp.Status, response)
		}

		// Parse response and write results
		ret := &queryResponse{}
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
		if err := result.validate(); err != nil {
			return err
		}
		textutil.WriteTable(ctx, result, headerRender)

		if timer {
			fmt.Printf("Run Time: %f seconds\n", result.Time)
		}
		return nil
	}
}
