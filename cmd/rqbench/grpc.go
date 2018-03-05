package main

import (
	"time"

	pb "github.com/rqlite/rqlite/grpc/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type GRPCTester struct {
	client  pb.RqliteClient
	addr    string
	message *pb.ExecRequest
}

func NewGRPCTester(addr string) *GRPCTester {
	return &GRPCTester{addr: addr}
}

func (g *GRPCTester) Prepare(stmt string, bSz int, tx bool) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	g.client = pb.NewRqliteClient(conn)

	s := make([]string, bSz)
	for i := 0; i < len(s); i++ {
		s[i] = stmt
	}

	g.message = &pb.ExecRequest{Stmt: s}
	return nil
}

func (g *GRPCTester) Once() (time.Duration, error) {
	start := time.Now()
	_, err := g.client.Exec(context.Background(), g.message)
	if err != nil {
		return 0, err
	}
	return time.Since(start), nil
}
