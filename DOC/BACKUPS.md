# Backups
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

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
Note that if the node is not the Leader, the node will transparently forward the request to Leader, wait for the backup data from the Leader, and return it to the client. If, instead, you want a backup of SQLite database of the actual node that receives the request, add `noleader` to the URL as a query parameter.

If you do not wish a Follower to transparently forward a backup request to a Leader, add `redirect` to the URL as a query parameter. In that case if a Follower receives a backup request the Follower will respond with [HTTP 301 Moved Permanently](https://en.wikipedia.org/wiki/HTTP_301) and include the address of the Leader as the `Location` header in the response. It is then up the clients to re-issue the command to the Leader.

In either case the generated file can then be used to restore a node (or cluster) using the [restore API](https://github.com/rqlite/rqlite/blob/master/DOC/RESTORE_FROM_SQLITE.md).

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
The isolation offered by binary backups is `READ COMMITTED`. This means that any changes due to transactions to the database, that take place during the backup, will be reflected immediately once the transaction is committed, but not before.

See the [SQLite documentation](https://www.sqlite.org/isolation.html) for more details.
