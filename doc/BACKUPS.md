# Backups

rqlite supports hot-backing up a node. You can retrieve and write a consistent snapshot of the underlying SQLite database to a file like so:

```bash
curl localhost:4001/db/backup -o bak.sqlite3
```

The node can then be restored by loading this database file via `sqlite3` and executing `.dump`. You can then use the output of the dump to replay the entire database back into brand new node (or cluster), *with the exception* of `BEGIN TRANSACTION` and `COMMIT` commands. You should ignore those commands in the `.dump` output.

By default a backup can only be retrieved from the leader, though this check can be disabled by adding `noleader` to the URL as a query param.
