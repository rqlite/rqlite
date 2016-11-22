# Backups

rqlite supports hot-backing up a node. You can retrieve and write a consistent snapshot of the underlying SQLite database to a file like so:

```bash
curl localhost:4001/db/backup -o bak.sqlite3
```

The node can then be restored by loading this database file via `sqlite3` and executing `.dump`. You can then use the output of this dump to replay the entire database back into brand new node (or cluster), *with the exception* of `BEGIN TRANSACTION` and `COMMIT` commands. You should ignore those commands in the `.dump` output, and not play them into the new cluster.

By default a backup can only be retrieved from the leader, though this check can be disabled by adding `noleader` to the URL as a query param.

## Example
An example backup and dump session is shown below.
```bash
~ $ curl localhost:4001/db/backup -o bak.sqlite3
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  2048  100  2048    0     0  35932      0 --:--:-- --:--:-- --:--:-- 36571
~ $ sqlite3 bak.sqlite3 .dump >dump.sql
~ $ cat dump.sql 
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE foo (id integer not null primary key, name text);
INSERT INTO "foo" VALUES(1,'fiona');
COMMIT;
~ $ 
```
To restore an rqlite system from the dump, issue each command between `BEGIN TRANSACTION` and `COMMIT` to rqlite, using the HTTP API.
