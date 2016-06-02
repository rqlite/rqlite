# Command Line Interface
rqlite comes with a CLI, which can make it easier to interact with cluster. It is installed in the same directory as the cluster binary `rqlited`. 

An example session is shown below.
```bash
$ rqlite
127.0.0.1:4001> CREATE TABLE foo (id integer not null primary key, name text)
0 row affected (0.000668 sec)
127.0.0.1:4001> INSERT INTO foo(name) VALUES("fiona")
1 row affected (0.000080 sec)
127.0.0.1:4001> SELECT * FROM foo
+----+-------+
| id | name  |
+----+-------+
| 1  | fiona |
+----+-------+
```
You can connect the CLI to any node in the cluster, and it will automatically forward its requests to the leader. Pass `-h` to `rqlite` to learn more.
