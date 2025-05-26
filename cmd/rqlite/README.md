# rqlite

`rqlite` is a command line tool for connecting to a rqlited node. Consult the [SQLite query language documentation](https://www.sqlite.org/lang.html) for full details on the supported SQL syntax.

- [Usage](#usage)
- [Example](#example)
- [Build](#example)

## Usage

```sh
$> rqlite -h
Options:

  -h, --help
      display help information

  -a, --alternatives
      comma separated list of 'host:port' pairs to use as fallback

  -s, --scheme[=http]
      protocol scheme (http or https)

  -H, --host[=127.0.0.1]
      rqlited host address

  -p, --port[=4001]
      rqlited host port

  -P, --prefix[=/]
      rqlited HTTP URL prefix

  -i, --insecure[=false]
      do not verify rqlited HTTPS certificate

  -c, --ca-cert
      path to trusted X.509 root CA certificate

  -u, --user
      set basic auth credentials in form username:password

  -v, --version
      display CLI version
```

### Environment Variables

The rqlite shell also supports configuration through environment variables:

- `RQLITE_HOST`: Connection string in the format `[scheme://]host[:port]`

Environment variables are only used when the corresponding command-line flags are not explicitly set. Command-line flags always take precedence over environment variables.

Examples:
```sh
# Connect to a host on a non-standard port
export RQLITE_HOST=example.com:8080
rqlite

# Connect using HTTPS
export RQLITE_HOST=https://secure.example.com
rqlite

# Command-line flags override environment variables
export RQLITE_HOST=example.com:8080
rqlite -H localhost -p 4001  # Will connect to localhost:4001, not example.com:8080
```

## Example
Connecting to a host running locally:
```sh
$ rqlite
127.0.0.1:4001> CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)
0 row affected (0.000362 sec)
127.0.0.1:4001> .tables
+------+
| name |
+------+
| foo  |
+------+
127.0.0.1:4001> .schema
+---------------------------------------------------------------+
| sql                                                           |
+---------------------------------------------------------------+
| CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT) |
+---------------------------------------------------------------+
127.0.0.1:4001> INSERT INTO foo(name) VALUES("fiona")
1 row affected (0.000117 sec)
127.0.0.1:4001> SELECT * FROM foo
+----+-------+
| id | name  |
+----+-------+
| 1  | fiona |
+----+-------+
127.0.0.1:4001> quit
bye~
```
Connecting to a host running somewhere else on the network:
```
$ rqlite -H localhost -p 8493
localhost:8493>
```

## Build

```sh
go build -o rqlite
```

