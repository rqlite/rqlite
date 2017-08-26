# Status and Diagnostics API
A status API exists, which dumps some diagnostic and statistical information, as well as basic information about the underlying Raft node. Assuming the rqlite node is started with default settings, node status is available like so:

```bash
curl localhost:4001/status?pretty
```

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

You can also request the same status information via the CLI:
```
$ rqlite
127.0.0.1:4001> .status
runtime:
  GOARCH: amd64
  GOMAXPROCS: 8
  GOOS: linux
  numCPU: 8
  numGoroutine: 13
  version: go1.8.1
store:
  db_conf:
    Memory: true
    DSN:
  peers.....
 ```

## expvar support
rqlite also exports [expvar](http://godoc.org/pkg/expvar/) information. The standard expvar information, as well as some custom information, is exposed. This data can be retrieved like so (assuming the node is started in its default configuration):

```bash
curl localhost:4001/debug/vars
```

You can also request the same expvar information via the CLI:
```
$ rqlite
127.0.0.1:4001> .expvar
cmdline: [./rqlited data]
db:
  execute_transactions: 0
  execution_errors: 1
  executions: 1
  queries: 0
  query_transactions: 0
http:
  backups: 0
  executions: 0
  queries: 0
memstats:
  Mallocs: 8950
  HeapSys: 2.588672e+06
  StackInuse: 557056
  LastGC: 0...
 ```

## pprof support
pprof information is available by default and can be retrieved as follows:

```bash
curl localhost:4001/debug/pprof/cmdline
curl localhost:4001/debug/pprof/profile
curl localhost:4001/debug/pprof/symbol
```
