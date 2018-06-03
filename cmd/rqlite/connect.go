package main

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/mkideal/cli"
)

func makeConnectRequest(_ string) func(string) (*http.Request, error) {
	return func(urlStr string) (*http.Request, error) {
		req, err := http.NewRequest("POST", urlStr, nil)
		if err != nil {
			return nil, err
		}
		return req, nil
	}
}

func connect(ctx *cli.Context, argv *argT) (uint64, error) {
	u := url.URL{
		Scheme: argv.Protocol,
		Host:   fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:   fmt.Sprintf("%sdb/connections", argv.Prefix),
	}
	_, err := sendRequest(ctx, makeConnectRequest("ignored"), u.String(), argv)
	if err != nil {
		return 0, err
	}
	return 0, nil
}
