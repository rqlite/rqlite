# Foreign Key Constraints
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

Since SQLite does not enforce foreign key constraints by default, neither does rqlite. However you can enable foreign key constraints in rqlite via the command line option `-fk=true`. Setting this command line will enable Foreign Key constraints on all connections that rqlite makes to the underlying SQLite database.

Issuing the `PRAGMA foreign_keys = boolean` command usually results in unpredictable behaviour, since rqlite doesn't offer connection-level control of the underlying SQLite database. It is not recommended.
