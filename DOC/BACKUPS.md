# Backups

rqlite supports hot backing up a node. You can retrieve and write a copy of the underlying SQLite database to a file via the CLI:
```
127.0.0.1:4001> .backup bak.sqlite3
backup file written successfully
```
This command will write the SQLite database file to `bak.sqlite3`.

You can also access the rqlite API directly, via a HTTP `GET` request to the endpoint `/db/backup`. For example, using `curl`, and assuming the node is listening on `localhost:4001`, you could retrieve a backup as follows:
```bash
curl -s -XGET localhost:4001/db/backup -o bak.sqlite3
```

In either case the generated file file can then be used to restore a node (or cluster) using the [restore API](https://github.com/rqlite/rqlite/blob/master/DOC/RESTORE_FROM_SQLITE.md).

## Generating a SQL text dump
You can dump the database in SQL text format via the CLI as follows:
```
127.0.0.1:4001> .dump bak.sql
SQL text file written successfully
```
The API can also be accessed directly:
```bash
curl -s -XGET localhost:4001/db/backup?fmt=sql -o bak.sql
```

## Backup isolation level
The isolation offered by backups is `READ COMMITTED`. This means that the changes due to a transaction on a connection to the database, including the connection used to backup the database, that take during the backup, will be reflected in the backup once the transaction is committed.

See the [SQLite documentation](https://www.sqlite.org/isolation.html) for more details.
