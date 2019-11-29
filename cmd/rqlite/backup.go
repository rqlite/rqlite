package main

import (
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/mkideal/cli"
)

type backupResponse struct {
	BackupFile []byte
}

func backup(ctx *cli.Context, filename string, argv *argT) error {
	queryStr := url.Values{}
	u := url.URL{
		Scheme:   argv.Protocol,
		Host:     fmt.Sprintf("%s:%d", argv.Host, argv.Port),
		Path:     fmt.Sprintf("%sdb/backup", argv.Prefix),
		RawQuery: queryStr.Encode(),
	}
	response, err := sendRequest(ctx, "GET", u.String(), "", argv)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filename, *response, 0644)
	if err != nil {
		return err
	}

	ctx.String("backup written successfully to %s\n", filename)
	return nil
}
