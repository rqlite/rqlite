# Status and Diagnostics API
A status API exists, which returns extensive diagnostic and statistical information, as well as basic information about the underlying Raft node. Assuming the rqlite node is started with default settings, node status is available via a HTTP `GET` request. To see the raw data, you can issue a `curl` command like so:

```bash
curl localhost:4001/status?pretty
```

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

You can also request the same status information via the CLI:
```
$ ./rqlite 
Welcome to the rqlite CLI. Enter ".help" for usage hints.
127.0.0.1:4001> .status
build:
  build_time: unknown
  commit: unknown
  version: 5
  branch: unknown
http:
  addr: 127.0.0.1:4001
  auth: disabled
  redirect: 
node:
  start_time: 2019-12-23T22:34:46.215507011-05:00
  uptime: 16.963009139s
runtime:
  num_goroutine: 9
  version: go1.13
 ```

 ## Nodes API
 The _nodes_ API returns basic information for nodes in the cluster, as seen by the node receiving the _nodes_ request. The receiving node will also check whether it can actually connect to all other nodes in the cluster. This is an effective way to determine the cluster leader, and the leader's HTTP API address. It can also be used to check if the cluster is **basically** running -- if the other nodes are reachable, it probably is.

 By default, the node only checks if _voting_ nodes are contactable.

```bash
curl localhost:4001/nodes?pretty
curl localhost:4001/nodes?nonvoters&pretty  # Also check non-voting nodes.
curl localhost:4001/nodes?timeout=5s  # Give up if all nodes don't respond within 5 seconds. Default is 30 seconds.
```

You can also request the same nodes information via the CLI:
```
$ ./rqlite
Welcome to the rqlite CLI. Enter ".help" for usage hints.
127.0.0.1:4001> .nodes
1:
  api_addr: http://localhost:4001
  addr: 127.0.0.1:4002
  reachable: true
  leader: true
2:
  api_addr: http://localhost:4003
  addr: 127.0.0.1:4004
  reachable: true
  leader: false
3:
  api_addr: http://localhost:4005
  addr: 127.0.0.1:4006
  reachable: true
  leader: false
 ```

 ## Readiness checks
 rqlite nodes serve a "ready" status at `/readyz`. The endpoint will return `HTTP 200 OK` if the node is ready to respond to database requests and cluster management operations. An example access is shown below.

 ```bash
 $ curl localhost:4001/readyz
[+]leader ok
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
[pprof](https://golang.org/pkg/net/http/pprof/) information is available by default and can be accessed as follows:

```bash
curl localhost:4001/debug/pprof/cmdline
curl localhost:4001/debug/pprof/profile
curl localhost:4001/debug/pprof/symbol
```
