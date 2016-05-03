# rqlite-cli

`rqlite-cli` is a command line tool for connecting rqlited.

## Build

```sh
go build -o rqlite-cli
```

## Usage

```sh
$> ./rqlite-cli -h
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

# start rqlite-cli terminal
$> ./rqlite-cli
# now, we have enter the rqlite-cli terminal
127.0.0.1:4001> create table foo (id integer not null primary key, name text)
{
    "last_insert_id": 2,
    "rows_affected": 1,
    "time": 0.00019249700000000002
}
127.0.0.1:4001> insert into foo(name) values("fiona")
{
    "last_insert_id": 1,
    "rows_affected": 1,
    "time": 0.000155756
}
127.0.0.1:4001> select * from foo
+----+-------+
| id | name  |
+----+-------+
| 1  | fiona |
+----+-------+
127.0.0.1:4001> quit
bye~
$>
```
