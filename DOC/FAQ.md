# Frequently asked questions

## What exactly does rqlite do?
rqlite is about replicating a set of data, which has been written to it using SQL. The data is replicated for fault tolerance i.e. your data is so important that you want multiple copies distributed in different places.

On top of that, rqlite provides strong guarantees about what state any copy of that data is in, with respect to the leader. That is where Raft comes in. It prevents divergent copies, and ensures there is an "authoritative" copy of that data at all times.

And, of course, it allows you to read your data back, via standard SQL.

## Why would I use this, versus some other replicated database?
rqlite is very simple to deploy, run, and manage. For example, it's a single binary you can drop anywhere on a machine, and just start it. This makes it very convenient. 

That said, it's always possible it's _too_ simple for your needs.

## rqlite is distributed. Does that mean it can increase SQLite performance?
Yes, but only for reads. It does not provide any scaling for writes, since all writes must go through the leader. rqlite is distributed for replication and fault tolerance, not for peformance. In fact write performance is reduced relative to a standalone SQLite database, because of the round-trips between nodes.

## Is it a drop-in replacement for SQLite?
No. While it does use SQLite as its storage engine, you must access the system via HTTP. That said, since it basically exposes SQLite, all the power of that database is available. It is also possible that any system built on top of SQLite only needs small changes to work with rqlite.

## Can I run a single node?
Sure. Many people do so, as they like accessing a SQLite database over HTTP. 

## Is rqlite a good match for a network of nodes that come and go -- perhaps thousands of them?
Unlikely. While rqlite does support read-only nodes, allowing it to scale to many nodes, the consensus protocol at the core of rqlite works best when the nodes in the cluster don't continually come and go. While it won't break, it probably won't be practical.

## How does it different than dqlite?
dqlite is library, written in C, that you can integrate with your own software. This requires programming. rqlite is a standalone program -- it's a full RDBMS (albeit a simple one).
