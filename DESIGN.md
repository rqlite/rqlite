# rqlite
You can find details on the design and implementation of rqlite from [these blog posts](http://www.philipotoole.com/tag/rqlite/).

## Node design
The diagram below shows a high-level view of a rqlite node.

                 ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐    ┌ ─ ─ ─ ─ ┐
                             Clients                   Other
                 └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘    │  Nodes  │
                                │                    ─ ─ ─ ─ ─
                                │                        ▲
                                │                        │
                                │                        │
                                ▼                        ▼
                 ┌─────────────────────────────┐ ┌───────────────┐
                 │           HTTP(S)           │ │      TCP      │
                 └─────────────────────────────┘ └───────────────┘
                 ┌───────────────────────────────────────────────┐
                 │             Raft (hashicorp/raft)             │
                 └───────────────────────────────────────────────┘
                 ┌───────────────────────────────────────────────┐
                 │               matt-n/go-sqlite3               │
                 └───────────────────────────────────────────────┘
                 ┌───────────────────────────────────────────────┐
                 │                   sqlite3.c                   │
                 └───────────────────────────────────────────────┘

## Log Compaction
rqlite automatically performs log compaction. After a fixed number of changes rqlite snapshots the SQLite database, and truncates the Raft log. This is a technical feature of the Raft consensus system, and most users of rqlite need not be concerned with this.
