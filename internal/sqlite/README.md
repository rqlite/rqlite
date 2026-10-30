# modernc experiment

This checkout replaces the mattn/rqlite SQLite driver with
[modernc.org/sqlite v1.61.0](https://pkg.go.dev/modernc.org/sqlite@v1.61.0).
There is no mattn SQLite dependency or CGO requirement. The existing Go 1.27
requirement is unchanged.

Build and profile from the repository root:

```sh
CGO_ENABLED=0 go build -o /tmp/rqlited-modernc ./cmd/rqlited
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go test ./db -run '^$' -bench BenchmarkDriverPaths -benchmem -benchtime=10s -cpuprofile=/tmp/modernc.cpu -memprofile=/tmp/modernc.mem
go tool pprof /tmp/modernc.cpu
/tmp/rqlited-modernc -cpu-profile /tmp/rqlited.cpu /tmp/rqlite-modernc-data
```

Stop the server gracefully to finish its CPU profile. Use a separate data
folder and the same workload/settings for baseline comparisons. Copy
`db/driver_bench_test.go` to the baseline checkout to run identical benchmarks.
These benchmarks measure rqlite's DB API and protobuf conversion, without
HTTP or Raft; the execute workload is a single-row UPDATE. They are smoke
benchmarks, not evidence of comparative performance.

The adapter preserves the API used by db, with normal execute/query operations
forwarded to modernc's database/sql driver. It translates SQLite errors and
wraps row iteration to preserve cancellation behavior. Existing DSNs work with
this version of modernc. Foreign keys, read-only connections, explicit
transactions, and parameter binding use the same rqlite paths as before.

SQLite's Go-translated low-level API implements statement read-only checks,
column origins, online backups between live connections, attach limits,
autocommit checks, checkpoint-on-close control, update hooks and trace_v2.
Modernc's exported hooks implement preupdate, commit and rollback callbacks.
Automatic checkpointing remains explicitly disabled on every connection.

This adapter uses reflection/unsafe to obtain modernc's private `db` and `tls`
fields once per connection. This is a deliberate experimental dependency on
v1.61.0 internals. Revalidate before upgrading; an upstream exported handle API
or a small modernc fork would be preferable for production. Raw calls require
serialized connection access, supplied by database/sql and Conn.Raw. Statement
classification currently prepares an additional metadata statement, which
adds overhead to the mixed request path.

Known limitations:

- Native SQLite shared-library extensions are rejected explicitly.
- Modernc's SQLite build does not include FTS3; use FTS5 where appropriate.
- SQLite compile options/version differ from the original driver. Connection
  pragmas implement rqlite's checkpoint and synchronous policy at runtime.
- This is an experimental Linux/amd64-validated port; portability and race
  instrumentation have not been validated.

Validation so far: all packages build with CGO disabled. The full db/... suite
finishes with only Test_DB_CompileOptions (old compile-time defaults) and
Test_DB_TableCreationFTS (FTS3) failing. This includes passing backup,
serialization, WAL replay, checkpoint manager, CDC, query logging, error,
execute/query and transaction tests. Extension-dependent tests may skip when
native test fixtures are unavailable. The new benchmark's three paths pass.

Additional validation: the db/... suite passes when those two expectations
are excluded (`-skip '^(Test_DB_CompileOptions|Test_DB_TableCreationFTS)$'`).
Single-node system tests passed, including HTTP execute/query, parameterized
and blob requests, backup/load and snapshot/reopen. Focused store snapshot,
failure-injection, vacuum and command-processor tests passed. Snapshot tests
selected by `Test_Snapshot` passed. A broader store run hit its overall five-minute
limit while progressing through tests; the full store suite is not certified.
The built `/tmp/rqlited-modernc` is a statically linked Linux/amd64 binary.
