package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/mkideal/cli"
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

func makeExecuteRequest(line string) func(string) (*http.Request, error) {
	requestData := strings.NewReader(makeJSONBody(line))
	return func(urlStr string) (*http.Request, error) {
		req, err := http.NewRequest("POST", urlStr, requestData)
		if err != nil {
			return nil, err
		}
		return req, nil
	}
}

func execute(ctx *cli.Context, cmd, line string, timer bool, argv *argT) error {
	queryStr := url.Values{}
	if timer {
		queryStr.Set("timings", "")
	}
	u := url.URL{
		Scheme:   argv.Protocol,
		Host:     fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:     fmt.Sprintf("%sdb/execute", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := sendRequest(ctx, makeExecuteRequest(line), u.String(), argv)
	if err != nil {
		return err
	}
	ret := &executeResponse{}
	if err := parseResponse(response, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return fmt.Errorf(ret.Error)
	}
	if len(ret.Results) != 1 {
		// What's happen? ret.Results.length MUST be 1
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
	ctx.String("%d %s affected (%f sec)\n", result.RowsAffected, rowString, result.Time)
	return nil
}
