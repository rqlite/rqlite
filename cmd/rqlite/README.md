# rqlite

`rqlite` is a command line tool for connecting to a rqlited node.

## Build

```sh
go build -o rqlite
```

## Usage

```sh
$> ./rqlite -h
Options:

  -h, --help
      display help

  -P, --scheme[=http]
      protocol scheme(http or https)

  -H, --host[=127.0.0.1]
      rqlited host address

  -p, --port[=4001]
      rqlited listening http(s) port
```

## Example

```sh
# start rqlited
$> rqlited ~/node.1

# start rqlite CLI
$> ./rqlite
# now, we have entered the rqlite terminal
127.0.0.1:4001> CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)
1 row affected (0.015 sec)
127.0.0.1:4001> INSERT INTO foo(name) VALUES("fiona")
1 row affected (0.055 sec)
127.0.0.1:4001> SELECT * FROM foo
+----+-------+
| id | name  |
+----+-------+
| 1  | fiona |
+----+-------+
127.0.0.1:4001> quit
bye~
$>
```
