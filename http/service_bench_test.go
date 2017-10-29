package http

import (
	"net/http"
	"strings"
	"testing"
)

func Benchmark_Exec(b *testing.B) {
	stmt := `["INSERT INTO foo(name) VALUES(\"fiona\")"]`
	client := http.Client{}

	for i := 0; i < b.N; i++ {
		resp, err := client.Post("http://localhost:4001/db/execute", "application/json", strings.NewReader(stmt))
		if err != nil {
			panic("failed to insert record")
		}
		resp.Body.Close()
	}
}
