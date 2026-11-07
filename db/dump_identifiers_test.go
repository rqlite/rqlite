package db

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func Test_DumpQuotedIdentifiers(t *testing.T) {
	for _, tc := range []struct{ name, table, column string }{
		{"column double quote", "items", `a"b`},
		{"column single quote", "items", "a'b"},
		{"table single quote", "a'b", "value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := Open(filepath.Join(t.TempDir(), "src.db"), false, true)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			ident := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
			mustExecute(src, fmt.Sprintf("CREATE TABLE %s (%s TEXT)", ident(tc.table), ident(tc.column)))
			mustExecute(src, fmt.Sprintf("INSERT INTO %s VALUES ('saved value')", ident(tc.table)))
			var dump bytes.Buffer
			if _, err := src.Dump(&dump); err != nil {
				t.Fatal(err)
			}
			dst, err := Open(filepath.Join(t.TempDir(), "dst.db"), false, true)
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			results, err := dst.ExecuteStringStmt(dump.String())
			if err != nil {
				t.Fatal(err)
			}
			for _, result := range results {
				if result.GetE().GetError() != "" {
					t.Fatalf("restore: %s; dump: %s", result.GetE().GetError(), dump.String())
				}
			}
			rows, err := dst.QueryStringStmt(fmt.Sprintf("SELECT %s FROM %s", ident(tc.column), ident(tc.table)))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].Error != "" || len(rows[0].Values) != 1 || rows[0].Values[0].Parameters[0].GetS() != "saved value" {
				t.Fatalf("dump lost data: %v; dump: %s", rows, dump.String())
			}
		})
	}
}
