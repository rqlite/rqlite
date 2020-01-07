# rqlite
You can find details on the design and implementation of rqlite from [these blog posts](http://www.philipotoole.com/tag/rqlite/).

The design and implementation of rqlite was also discussed at the [GoSF](http://www.meetup.com/golangsf/) [April 2016](http://www.meetup.com/golangsf/events/230127735/) Meetup. You can find the slides [here](http://www.slideshare.net/PhilipOToole/rqlite-replicating-sqlite-via-raft-consensu). A similar talk was given to the University of Pittsburgh in April 2018. Those slides are [here](https://docs.google.com/presentation/d/1lSNrZJUbAGD-ZsfD8B6_VPLVjq5zb7SlJMzDblq2yzU/edit?usp=sharing).

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
The Raft layer always creates a file -- it creates the _Raft log_. The log stores the set of commited SQLite commands, in the order which they were executed. This log is authoritative record of every change that has happened to the system. It may also contain some read-only queries entries, depending on read-consistency choices.

### SQLite
By default the SQLite layer doesn't create a file. Instead it creates the database in RAM. rqlite can create the SQLite database on disk, if so configured at start-time.

## Log Compaction and Truncation
rqlite automatically performs log compaction, so that disk usage due to the log remains bounded. After a configurable number of changes rqlite snapshots the SQLite database, and truncates the Raft log. This is a technical feature of the Raft consensus system, and most users of rqlite need not be concerned with this.

## Distributed Consensus 
The following provides detailed information related to Raft, distributed consensus, and rqlite.

### rqlite and the CAP theorem
The [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem) states that it is impossible for a distributed database to provide consistency, availability, and partition tolerance simulataneously -- that, in the face of a network partition, the database can be available or consistent, but not both.

Raft is a Consistency-Partition (CP) protocol. This means that if a rqlite cluster is partitioned, only the side of the cluster that contains a majority of the nodes will be available. The other side of the cluster will not respond to writes. However the side that remains available will return consistent results, and when the partition is healed, consistent results will continue to be returned.

### Does the protocol require consensus be reached before a commit is accepted?
Yes, this is an intrinsic part of the Raft protocol. How long it takes to reach consensus depends primarily on your network. It will take two rounds trips from a leader to a quorum of nodes, though each of those nodes is contacted in parallel.

### Is the underlying serializable isolation level of SQLite maintained?
Yes, it is.

### Do concurrent writes block each other? 
In this regard rqlite currently offers exactly the same semantics as SQLite. Each HTTP write request uses the same SQLite connection on the leader, so one write-over-HTTP may block another. Explicit connection control will be available in a future release, which will clients more control over transactions. Only one concurrent write will ever be supported however, due to the nature of SQLite.

### How does this solution scale?
The simplest way to scale for reads and writes is to use higher-performance disks and a lower-latency network. This is known as _scaling vertically_.

rqlite doesn't scale horizontally for writes however, as all writes must go through the leader. It can be scaled horizontally for reads though, via [read-only nodes](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md).

### How does a client detect a partition?
If the client is on the same side of the partition as a quorum of nodes, there will be no real problem, and any writes should succeed. However if the client is on the other side of the partition, it will still be redirected to the leader, but will then (presumably) fail to contact the leader, and experience a timeout. It may be possible to make this condition clearer to clients in a future release.
