package db

import (
	command "github.com/rqlite/rqlite/v10/command/proto"
	"path/filepath"
	"testing"
)

// BenchmarkDriverPaths exercises the public database API, including parameter
// conversion and protobuf results. Copy this file to the baseline checkout for
// a like-for-like comparison; setup is excluded from the measurements.
func BenchmarkDriverPaths(b *testing.B) {
	for _, path := range []string{"execute", "query", "request"} {
		b.Run(path, func(b *testing.B) {
			d, err := Open(filepath.Join(b.TempDir(), "bench.db"), false, true)
			if err != nil {
				b.Fatal(err)
			}
			defer d.Close()
			for _, q := range []string{"CREATE TABLE bench(id INTEGER PRIMARY KEY, value TEXT)", "INSERT INTO bench VALUES(1, 'value')"} {
				r, e := d.ExecuteStringStmt(q)
				if e != nil {
					b.Fatal(e)
				}
				if r[0].GetE().GetError() != "" {
					b.Fatal(r)
				}
			}
			req := &command.Request{Statements: []*command.Statement{{Sql: "SELECT value FROM bench WHERE id = ?", Parameters: []*command.Parameter{{Value: &command.Parameter_I{I: 1}}}}}}
			if path == "execute" {
				req.Statements[0].Sql = "UPDATE bench SET value = 'value' WHERE id = ?"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if path == "query" {
					r, e := d.Query(req, false)
					if e != nil {
						b.Fatal(e)
					}
					if r[0].Error != "" {
						b.Fatal(r)
					}
				} else {
					var r []*command.ExecuteQueryResponse
					var e error
					if path == "execute" {
						r, e = d.Execute(req, false)
					} else {
						r, e = d.Request(req, false)
					}
					if e != nil {
						b.Fatal(e)
					}
					if r[0].GetE().GetError() != "" || r[0].GetQ().GetError() != "" {
						b.Fatal(r)
					}
				}
			}
		})
	}
}
