# rqlite
You can find details on the design and implementation of rqlite from [these blog posts](http://www.philipotoole.com/tag/rqlite/).

rqlite was also discussed at the [GoSF](http://www.meetup.com/golangsf/) [April 2016](http://www.meetup.com/golangsf/events/230127735/) Meetup. You can find the slides [here](http://www.slideshare.net/PhilipOToole/rqlite-replicating-sqlite-via-raft-consensu).

## Node design
The diagram below shows a high-level view of a rqlite node.

                 ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐             ┌ ─ ─ ─ ─ ┐
                             Clients                            Other
                 └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘             │  Nodes  │
                                │                             ─ ─ ─ ─ ─
                                │                                 ▲
                                │                                 │
                                │                                 │
                                ▼                                 ▼
                 ┌─────────────────────────────┐ ┌─────────────────────────────────────┐
                 │           HTTP(S)           │ │                  TCP                │
                 └─────────────────────────────┘ └─────────────────────────────────────┘
                 ┌───────────────────────────────────────────────┐┌────────────────────┐
                 │             Raft (hashicorp/raft)             ││   Internode meta   │
                 └───────────────────────────────────────────────┘└────────────────────┘
                 ┌───────────────────────────────────────────────┐
                 │               matt-n/go-sqlite3               │
                 └───────────────────────────────────────────────┘
                 ┌───────────────────────────────────────────────┐
                 │                   sqlite3.c                   │
                 └───────────────────────────────────────────────┘
                 ┌───────────────────────────────────────────────┐
                 │                 RAM or disk                   │
                 └───────────────────────────────────────────────┘

## Log Compaction
rqlite automatically performs log compaction. After a configurable number of changes rqlite snapshots the SQLite database, and truncates the Raft log. This is a technical feature of the Raft consensus system, and most users of rqlite need not be concerned with this.
