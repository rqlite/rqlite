# Foreign Key Constraints

SQLite does not, currently, enforce foreign key constraints by default. However you can enable foreign key constraints on rqlite simply by sending `PRAGMA foreign_keys=ON` as a command. Constraints will remain enabled, even across restarts, unless the command `PRAGMA foreign_keys=OFF` is issued.

You can check the current state of foreign key constraints at anytime via the [status API](https://github.com/rqlite/rqlite/blob/master/doc/DIAGNOSTICS.md).