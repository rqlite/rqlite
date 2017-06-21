package main

import (
	"fmt"

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

func execute(ctx *cli.Context, cmd, line string, argv *argT) error {
	urlStr := fmt.Sprintf("%s://%s:%d%sdb/execute", argv.Protocol, argv.Host, argv.Port, argv.Prefix)
	ret := &executeResponse{}
	if err := sendRequest(ctx, urlStr, line, argv, ret); err != nil {
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
