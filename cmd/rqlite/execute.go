package main

import (
	"fmt"
	"net/url"

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

	response, err := sendRequest(ctx, "POST", u.String(), line, argv)
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
	return nil
}
