package grpc

import (
	"testing"

	pb "github.com/rqlite/rqlite/grpc/proto"
	"golang.org/x/net/context"
)

func Benchmark_Exec(b *testing.B) {
	g := mustGrpcClient("localhost:5001")

	for i := 0; i < b.N; i++ {
		_, err := g.Exec(context.Background(),
			&pb.ExecRequest{
				Stmt: []string{`INSERT INTO foo(name) VALUES("fiona")`},
			})
		if err != nil {
			panic("failed to insert record")
		}
	}
}
