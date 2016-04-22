# Status and Diagnotics API
A status API exists, which dumps some basic diagnostic and statistical information, as well as basic information about the underlying Raft node. Assuming rqlite is started with default settings, rqlite status is available like so:

```bash
curl localhost:4001/status?pretty
```

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

## expvar support
rqlite also exports [expvar](http://godoc.org/pkg/expvar/) information. The standard, and some custom information, is exposed. This data can be retrieved like so:
