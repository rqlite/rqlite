# Command Line Interface
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

rqlite comes with a CLI, which makes it easier to interact with a rqlite system. It is installed in the same directory as the node binary `rqlited`. Since rqlite is built on SQLite, you should consult the [SQLite query language documentation](https://www.sqlite.org/lang.html) for full details on what is supported.

> **⚠ WARNING: Only enter one command at a time at CLI. Don't enter multiple commands at once, separated by ;**  
> While it may work, mixing reads and writes to the database in a single CLI command results in undefined behavior.

An example session is shown below.
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
You can connect the CLI to any node in a cluster, and it will automatically forward its requests to the leader if needed. Pass `-h` to `rqlite` to learn more.

## History
Command history is stored and reload between sessions, in a hidden file in the user's home directory named `.rqlite_history`. By default 100 previous commands are stored, though the value can be explicitly set via the environment variable `RQLITE_HISTFILESIZE`.
