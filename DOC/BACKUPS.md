# Backups

rqlite supports hot backing up a node. You can retrieve and write a consistent snapshot of the underlying SQLite database to a file via a HTTP `GET` request to the endpoint `/db/backup`.

For example, using `curl`, and assuming the node is listening on `localhost:4001`, you could retrieve a backup as follows:
```bash
curl -s -XGET localhost:4001/db/backup -o bak.sqlite3
```
This file can then be used to restore a node (or cluster) using the [restore API](https://github.com/rqlite/rqlite/blob/master/DOC/RESTORE_FROM_SQLITE.md).
