# rqlite
You can find details on the design and implementation of rqlite from [these blog posts](http://www.philipotoole.com/tag/rqlite/) (in particular [this post](https://www.philipotoole.com/replicating-sqlite-using-raft-consensus/) and [this post](https://www.philipotoole.com/rqlite-replicated-sqlite-with-new-raft-consensus-and-api/).

The design and implementation of rqlite was also presented at the [GoSF](http://www.meetup.com/golangsf/) [April 2016](http://www.meetup.com/golangsf/events/230127735/) Meetup. You can find the slides [here](http://www.slideshare.net/PhilipOToole/rqlite-replicating-sqlite-via-raft-consensu). A similar talk was given to the University of Pittsburgh in April 2018. Those slides are [here](https://docs.google.com/presentation/d/1lSNrZJUbAGD-ZsfD8B6_VPLVjq5zb7SlJMzDblq2yzU/edit?usp=sharing).

## Node design
The diagram below shows a high-level view of an rqlite node.

                 ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐     ┌ ─ ─ ─ ─ ┐
                             Clients                    Other
                 └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘     │  Nodes  │
                                │                     ─ ─ ─ ─ ─
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
                 ┌───────────────────────────────────────────────┐
                 │                 RAM or disk                   │
                 └───────────────────────────────────────────────┘
## File system
### Raft
The Raft layer always creates a file -- it creates the _Raft log_. The log stores the set of committed SQLite commands, in the order which they were executed. This log is authoritative record of every change that has happened to the system. It may also contain some read-only queries as entries, depending on read-consistency choices.

### SQLite
By default the SQLite layer doesn't create a file. Instead it creates the database in RAM. rqlite can create the SQLite database on disk, if so configured at start-time.

## Log Compaction and Truncation
rqlite automatically performs log compaction, so that disk usage due to the log remains bounded. After a configurable number of changes rqlite snapshots the SQLite database, and truncates the Raft log. This is a technical feature of the Raft consensus system, and most users of rqlite need not be concerned with this.
