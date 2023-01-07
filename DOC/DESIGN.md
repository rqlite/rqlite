# rqlite
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

You can find details on the design and implementation of rqlite from [these blog posts](https://www.philipotoole.com/tag/rqlite/) (in particular [this post](https://www.philipotoole.com/replicating-sqlite-using-raft-consensus/) and [this post](https://www.philipotoole.com/rqlite-replicated-sqlite-with-new-raft-consensus-and-api/)).

## Design Presentations
- [Presentation](https://docs.google.com/presentation/d/1E0MpQbUA6JOP2GjA60CNN0ER8fia0TP6kdJ41U9Jdy4/edit#slide=id.p) given to Hacker Nights NYC, March 2022.
- [Presentation]( https://www.philipotoole.com/2021-rqlite-cmu-tech-talk) given to the [Carnegie Mellon Database Group](https://db.cs.cmu.edu/), [September 2021](https://db.cs.cmu.edu/events/vaccination-2021-rqlite-the-distributed-database-built-on-raft-and-sqlite-philip-otoole/). There is also a [video recording](https://www.youtube.com/watch?v=JLlIAWjvHxM) of the talk.
- [Presentation](https://docs.google.com/presentation/d/1lSNrZJUbAGD-ZsfD8B6_VPLVjq5zb7SlJMzDblq2yzU/edit?usp=sharing) given to the University of Pittsburgh, April 2018.
- [Presentation](https://www.slideshare.net/PhilipOToole/rqlite-replicating-sqlite-via-raft-consensu) given at the [GoSF](https://www.meetup.com/golangsf/) [April 2016](https://www.meetup.com/golangsf/events/230127735/) Meetup.

## Node design
The diagram below shows a high-level view of a rqlite node.
![node-design](https://user-images.githubusercontent.com/536312/133258366-1f2fbc50-8493-4ba6-8d62-04c57e39eb6f.png)

## File system
### Raft
The Raft layer always creates a file -- it creates the _Raft log_. This log stores the set of committed SQLite commands, in the order which they were executed. This log is authoritative record of every change that has happened to the system. It may also contain some read-only queries as entries, depending on read-consistency choices. Since every node in an rqlite cluster applies the entries log in exactly the same way, this guarantees that the SQLite database is the same on every node.

### SQLite
By default, the SQLite layer doesn't create a file. Instead, it creates the database in memory. rqlite can create the SQLite database on disk, if so configured at start-time, by passing `-on-disk` to `rqlited` at startup. Regardless of whether rqlite creates a database entirely in memory, or on disk, the SQLite database is completely recreated everytime `rqlited` starts, using the information stored in the Raft log.

## Log Compaction and Truncation
rqlite automatically performs log compaction, so that disk usage due to the log remains bounded. After a configurable number of changes rqlite snapshots the SQLite database, and truncates the Raft log. This is a technical feature of the Raft consensus system, and most users of rqlite need not be concerned with this.
