# Foreign Key Constraints

Since SQLite does not enforce foreign key constraints by default, neither does rqlite. However you can enable foreign key constraints on rqlite simply by sending `PRAGMA foreign_keys=ON` via the [CLI](https://github.com/rqlite/rqlite/tree/master/cmd/rqlite) or the [write API](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md#writing-data). Constraints will then remain enabled, even across restarts, until the statement `PRAGMA foreign_keys=OFF` is issued.

You can check the current state of foreign key constraints at anytime via the [status API](https://github.com/rqlite/rqlite/blob/master/DOC/DIAGNOSTICS.md), or by sending the SQL query `PRAGMA foreign_keys` to the [query API](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md#querying-data).
