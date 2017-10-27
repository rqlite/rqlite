package service

import (
	"database/sql"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/rqlite/rqlite/store"
)

// Store is the interface the Raft-based database must implement.
type Store interface {
	// Execute executes a slice of queries, each of which is not expected
	// to return rows. If timings is true, then timing information will
	// be return. If tx is true, then either all queries will be executed
	// successfully or it will as though none executed.
	Execute(queries []string, timings, tx bool) ([]*sql.Result, error)

	// Query executes a slice of queries, each of which returns rows. If
	// timings is true, then timing information will be returned. If tx
	// is true, then all queries will take place while a read transaction
	// is held on the database.
	Query(queries []string, timings, tx bool, lvl store.ConsistencyLevel) ([]*sql.Rows, error)

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

// New returns an instantiated service.
func New(addr string, store Store, credentials CredentialStore) *Service {
	s := Service{
		grpc:            grpc.NewServer(),
		store:           Store,
		credentialStore: CredentialStore,
		addr:            addr,
		logger:          log.New(os.Stderr, "[grpc] ", log.LstdFlags),
	}

	pb.RegisterRqliteServiceServer(s.grpc, (*gprcService)(&s))
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
	s.logger.Println("listening on", s.ln.Addr().String())

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

// gprcService is an unexported type, that is the same type as Service.
//
// Having the methods that the gRPC service requires on this type means that even though
// the methods are exported, since the type is not, these methods are not visible outside
// this package.
type gprcService Service

// Query implements the Query call on the gRPC service.
func (g *gprcService) Query(c context.Context, q *pb.QueryRequest) (*pb.QueryResponse, error) {
	return nil, nil
}

// Exec implements the Exec call on the gRPC service.
func (g *gprcService) Exec(c context.Context, e *pb.ExecRequest) (*pb.ExecResponse, error) {
	return nil
}

// Leader implements the Leader call on the gRPC service.
func (g *gprcService) Leader(c context.Context) (string, error) {
	return nil
}
