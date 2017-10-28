package grpc

import (
	"expvar"
	"log"
	"net"
	"os"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/rqlite/rqlite/db"
	pb "github.com/rqlite/rqlite/grpc/proto"
	"github.com/rqlite/rqlite/store"
)

// stats captures stats for the gRPC service.
var stats *expvar.Map

const (
	numExecutions     = "executions"
	numQueries        = "queries"
	numLeaderRequests = "leaderRequests"
)

// Store is the interface the Raft-based database must implement.
type Store interface {
	// Execute executes a slice of queries, each of which is not expected
	// to return rows. If timings is true, then timing information will
	// be return. If tx is true, then either all queries will be executed
	// successfully or it will as though none executed.
	Execute(queries []string, timings, tx bool) ([]*db.Result, error)

	// Query executes a slice of queries, each of which returns rows. If
	// timings is true, then timing information will be returned. If tx
	// is true, then all queries will take place while a read transaction
	// is held on the database.
	Query(queries []string, timings, tx bool, lvl store.ConsistencyLevel) ([]*db.Rows, error)

	// Leader returns the Raft leader of the cluster.
	Leader() string

	// Peer returns the API peer for the given address
	Peer(addr string) string
}

// CredentialStore is the interface credential stores must support.
type CredentialStore interface {
	// Check returns whether username and password are a valid combination.
	Check(username, password string) bool

	// HasPerm returns whether username has the given perm.
	HasPerm(username string, perm string) bool
}

// Service represents a gRPC service that communicates with Store.
type Service struct {
	grpc            *grpc.Server
	store           Store
	credentialStore CredentialStore

	ln   net.Listener
	addr string

	logger *log.Logger
}

func init() {
	stats = expvar.NewMap("grpc")
	stats.Add(numExecutions, 0)
	stats.Add(numQueries, 0)
	stats.Add(numLeaderRequests, 0)
}

// New returns an instantiated grpc service.
func New(addr string, store Store, credentials CredentialStore) *Service {
	s := Service{
		grpc:            grpc.NewServer(),
		store:           store,
		credentialStore: credentials,
		addr:            addr,
		logger:          log.New(os.Stderr, "[grpc] ", log.LstdFlags),
	}

	pb.RegisterRqliteServer(s.grpc, (*gprcService)(&s))
	return &s
}

// Addr returns the address on which the service is listening.
func (s *Service) Addr() string {
	return s.ln.Addr().String()
}

// Open opens the service, starting it listening on the configured address.
func (s *Service) Open() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.logger.Println("service listening on", s.ln.Addr().String())

	go func() {
		err := s.grpc.Serve(s.ln)
		if err != nil {
			s.logger.Println("gRPC Serve() returned:", err.Error())
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	s.grpc.GracefulStop()
	s.ln = nil
	s.logger.Println("gRPC server stopped")
	return nil
}

// Stats returns status of the gRPC server
func (s *Service) Stats() (interface{}, error) {
	var m map[string]string
	if s != nil {
		m = map[string]string{
			"state": "enabled",
			"addr":  s.addr,
			"auth":  prettyEnabled(s.credentialStore != nil),
		}
	} else {
		m = map[string]string{
			"state": "disabled",
		}
	}

	return m, nil

}

// gprcService is an unexported type, that is the same type as Service.
//
// Having the methods that the gRPC service requires on this type means that even though
// the methods are exported, since the type is not, these methods are not visible outside
// this package.
type gprcService Service

// Query implements the Query call on the gRPC service.
func (g *gprcService) Query(c context.Context, q *pb.QueryRequest) (*pb.QueryResponse, error) {
	stats.Add(numQueries, 1)

	return nil, nil
}

// Exec implements the Exec call on the gRPC service.
func (g *gprcService) Exec(c context.Context, e *pb.ExecRequest) (*pb.ExecResponse, error) {
	stats.Add(numExecutions, 1)

	start := time.Now()
	dbResults, err := g.store.Execute(e.GetStmt(), true, e.GetTx())
	if err != nil {
		return nil, err
	}

	execResults := make([]*pb.ExecResult, len(dbResults))
	for _, dr := range dbResults {
		execResults = append(execResults,
			&pb.ExecResult{
				LastInsertId: dr.LastInsertID,
				RowsAffected: dr.RowsAffected,
				Error:        dr.Error,
				Time:         dr.Time,
			})
	}

	return &pb.ExecResponse{
		Results: execResults,
		Time:    time.Now().Sub(start).Seconds(),
	}, nil
}

// Leader implements the Leader call on the gRPC service.
func (g *gprcService) Leader(c context.Context, r *pb.LeaderRequest) (*pb.LeaderResponse, error) {
	stats.Add(numLeaderRequests, 1)

	return &pb.LeaderResponse{Leader: g.store.Leader()}, nil
}

func prettyEnabled(e bool) string {
	if e {
		return "enabled"
	}
	return "disabled"
}
