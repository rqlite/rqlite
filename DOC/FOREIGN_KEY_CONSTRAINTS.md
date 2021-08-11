# Foreign Key Constraints

Since SQLite does not enforce foreign key constraints by default, neither does rqlite. However you can enable foreign key constraints in rqlite via the command line option `-fk=true`. Setting this command line will enable Foreign Key constraints on all connections that rqlite makes to the underlying SQLite database.

Issuing the `PRAGMA foreign_keys = boolean` results in undefined behavior.
