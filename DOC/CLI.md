# Command Line Interface
rqlite comes with a CLI, which makes it easier to interact with a rqlite system. It is installed in the same directory as the node binary `rqlited`. 

An example session is shown below.
```sh
$ ./rqlite 
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
You can connect the CLI to any node in a cluster, and it will automatically forward its requests to the leader. Pass `-h` to `rqlite` to learn more.
