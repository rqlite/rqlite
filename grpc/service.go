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
	grpc  *grpc.Server
	store Store

	ln   net.Listener
	addr string

	logger *log.Logger
}
