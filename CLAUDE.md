# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build all binaries
go install ./...

# Run all tests
go test ./...

# Run tests with race detection
go test -race ./...

# Run a single package's tests
go test ./store
go test ./db

# Run a specific test
go test -run TestStoreSingleNode ./store

# Lint (must pass before commits)
go vet ./...
gofmt -l .

# Format code (always run after modifying Go files)
go fmt ./...
```

## End-to-End Tests

E2E tests require building the binaries first:
```bash
go install ./...
RQLITED_PATH=$(go env GOPATH)/bin/rqlited python3 system_test/e2e/single_node.py
RQLITED_PATH=$(go env GOPATH)/bin/rqlited python3 system_test/e2e/multi_node.py
```

## Architecture Overview

rqlite is a distributed relational database built on SQLite with Raft consensus. Key design principle: single binary with no external dependencies.

### Core Packages

- **store/** - Central component managing Raft consensus. Contains FSM (Finite State Machine) that applies commands to SQLite. Handles Execute (writes), Query (reads), snapshots, and cluster coordination.

- **db/** - SQLite abstraction layer. Manages WAL (Write-Ahead Logging), checkpoints, and database hooks for CDC. Uses a forked go-sqlite3 (`github.com/rqlite/go-sqlite3`).

- **http/** - REST API server. Endpoints: `/db/execute` (writes), `/db/query` (reads), `/db/request` (unified). Handles consistency levels, authentication, and node status.

- **cluster/** - Inter-node communication using protobuf over TCP. Handles request forwarding to leader, node joins/removes.

- **command/** - Protobuf definitions for all operations (execute, query, backup, etc.). Located in `command/proto/`.

- **snapshot/** - Raft snapshot management. Supports full and incremental snapshots for efficient log truncation.

- **cdc/** - Change Data Capture. Hooks into SQLite preupdate/update callbacks, delivers changes to webhooks.

### Data Flow

**Writes**: HTTP request → Store (Raft consensus) → FSM applies to SQLite → Response

**Reads**: HTTP request → Consistency check → Query SQLite → Response

Consistency levels: none, weak, linearizable, strong (leader-only reads)

### Entry Points

- `cmd/rqlited/` - Server binary
- `cmd/rqlite/` - CLI client
- `cmd/rqbench/` - Benchmarking tool

### Key Dependencies

- `github.com/hashicorp/raft` - Consensus algorithm
- `github.com/rqlite/go-sqlite3` - Forked SQLite driver with additional hooks
- `go.etcd.io/bbolt` - BoltDB for Raft log storage
- `google.golang.org/protobuf` - Protocol buffer messages

## API and Consistency Model

### HTTP Endpoints
- `/db/execute` - Write operations (INSERT, UPDATE, DELETE)
- `/db/query` - Read operations (SELECT)
- `/db/request` - Unified endpoint accepting both reads and writes

### Read Consistency Levels (via `level` query parameter)
- **weak** (default) - Fast, checks local leadership state, may have ~1s staleness
- **linearizable** - Verifies leadership via quorum heartbeat, ensures up-to-date reads
- **none** - Direct local reads, fastest but may be stale; use `freshness` param to bound staleness
- **strong** - Query goes through Raft log, slowest, mainly for testing

### Write Modes
- **Standard** - Waits for Raft consensus before returning
- **Queued** (`?queue`) - Returns immediately, batched automatically, orders of magnitude faster
- **Bulk** - Multiple statements in single request/Raft entry for better throughput

### Non-deterministic Function Handling
Functions like `RANDOM()`, `datetime('now')` are rewritten before Raft log storage to ensure identical results across all replicas. Disable with `?norwrandom` or `?norwtime`.

## Clustering

- Raft quorum requires `(N/2)+1` nodes (3-node cluster tolerates 1 failure, 5-node tolerates 2)
- Auto-discovery via DNS, DNS-SRV, Consul, or etcd
- Manual clustering via `-join` flag or `-bootstrap-expect N` for simultaneous startup
- **Read-only nodes** (`-raft-non-voter`): Receive replication but don't vote, scale read throughput

## Operational Features

- **Hot backup**: `/db/backup` endpoint, supports SQL dump (`?fmt=sql`) and compression (`?compress`)
- **Auto-backup**: Periodic backups to S3, GCS, MinIO, or local filesystem
- **Restore**: `/boot` (fast, single-node) or `/db/load` (works on clusters)
- **CDC**: Streams INSERT/UPDATE/DELETE to webhooks with at-least-once delivery
- **Extensions**: Load SQLite extensions via `-extensions-path`

## Performance Notes

- Disk I/O (Raft fsync) is the primary bottleneck
- Queued writes dramatically improve throughput with small durability tradeoff
- SQLite runs in WAL mode with `SYNCHRONOUS=off` (Raft provides durability)
- Snapshot tuning: `-raft-snap` (entries), `-raft-snap-wal-size` (WAL size threshold)

## Code Style Guidelines

- Always run `go fmt` on modified Go source files
- Prefer standard library packages over third-party dependencies
- If using a third-party package, explain why it's necessary
