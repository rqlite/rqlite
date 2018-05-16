# In-memory databases
By default rqlite uses an [in-memory SQLite database](https://www.sqlite.org/inmemorydb.html) to maximise performance. In this mode no actual SQLite file is created and the entire database is stored in memory. If you wish rqlite to use an actual file-based SQLite database, pass `-on-disk` to `rqlited` on start-up.

## Does using an in-memory database put my data at risk?
No.

Since the Raft log is the authoritative store for all data, and it is written to disk by each node, an in-memory database can be fully recreated on start-up. Using an in-memory database does not put your data at risk.
