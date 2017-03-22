# Backups

rqlite supports hot backing up a node. You can retrieve and write a consistent snapshot of the underlying SQLite database to a file like so:

```bash
curl localhost:4001/db/backup -o bak.sqlite3
```

This file can then be used to restore a node (or cluster) using the [restore API](https://github.com/rqlite/rqlite/blob/master/doc/RESTORE_FROM_SQLITE.md).
