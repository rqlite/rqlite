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
	urlStr := fmt.Sprintf("%s://%s:%d/db/execute?pretty&timings", argv.Protocol, argv.Host, argv.Port)
	ret := &executeResponse{}
	if err := sendRequest(ctx, urlStr, line, ret); err != nil {
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
	ctx.JSONIndentln(result, "", "    ")
	return nil
}
