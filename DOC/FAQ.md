# FAQ

* [What exactly does rqlite do?](#what-exactly-does-rqlite-do)
* [Why would I use this, versus some other replicated database?](#why-would-i-use-this-versus-some-other-replicated-database)
* [Can any node execute a write request, and have the system "synchronize it all"?](#can-any-node-execute-a-write-request-and-have-the-system-synchronize-it-all)
* [rqlite is distributed. Does that mean it can increase SQLite performance?](#rqlite-is-distributed-does-that-mean-it-can-increase-sqlite-performance)
* [What is the best way to increase rqlite performance?](#what-is-the-best-way-to-increase-rqlite-performance)
* [Where does rqlite fit into the CAP theorem?](#where-does-rqlite-fit-into-the-cap-theorem)
* [Does the rqlite require consensus be reached before a write is accepted?](#does-the-rqlite-require-consensus-be-reached-before-a-write-is-accepted)
* [How does a client detect a cluster partition?](#how-does-a-client-detect-a-cluster-partition)
* [Can I run a single node?](#can-i-run-a-single-node)
* [Is rqlite a good match for a network of nodes that come and go -- perhaps thousands of them?](#is-rqlite-a-good-match-for-a-network-of-nodes-that-come-and-go----perhaps-thousands-of-them)
* [Is it a drop-in replacement for SQLite?](#is-it-a-drop-in-replacement-for-sqlite)
* [Can I modify the SQLite file directly?](#can-i-modify-the-sqlite-file-directly
* [Can I use rqlite to replicate my SQLite database to a second node?](#can-i-use-rqlite-to-replicate-my-sqlite-database-to-a-second-node)
* [Is the underlying serializable isolation level of SQLite maintained?](#is-the-underlying-serializable-isolation-level-of-sqlite-maintained)
* [Do concurrent writes block each other?](#do-concurrent-writes-block-each-other)
* [How is it different than dqlite?](#how-is-it-different-than-dqlite)

## What exactly does rqlite do?
rqlite is about replicating a set of data, which has been written to it using SQL. The data is replicated for fault tolerance because your data is so important that you want multiple copies distributed in different places, you want be able to query your data even if some machines fail, or both. These different places could be different machines on a rack, or different machines, each in different buildings, or even different machines, each on different continents.

On top of that, rqlite provides strong guarantees about what state any copy of that data is in, with respect to a special node called the _leader_. That is where Raft comes in. It prevents divergent copies of the data, and ensures there is an "authoritative" copy of that data at all times.

## Why would I use this, versus some other replicated database?
rqlite is very simple to deploy, run, and manage. For example, it's a single binary you can drop anywhere on a machine, and just start it. This makes it very convenient. 

That said, it's always possible it's _too_ simple for your needs.

## Can any node execute a write request, and have the system "synchronize it all"?
No, only the leader can make changes to the database. A client can _send_ a write-request to any node, and if that node is not the leader, the node will respond with the address of the leader, allowing the client to resend the request to the actual leader.

## rqlite is distributed. Does that mean it can increase SQLite performance?
Yes, but only for reads. It does not provide any scaling for writes, since all writes must go through the leader. rqlite is distributed for replication and fault tolerance, not for peformance. In fact write performance is reduced relative to a standalone SQLite database, because of the round-trips between nodes.

## What is the best way to increase rqlite performance?
The simplest way to increase performance is to use higher-performance disks and a lower-latency network. This is known as _scaling vertically_.

## Where does rqlite fit into the CAP theorem?
The [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem) states that it is impossible for a distributed database to provide consistency, availability, and partition tolerance simulataneously -- that, in the face of a network partition, the database can be available or consistent, but not both.

Raft is a Consistency-Partition (CP) protocol. This means that if a rqlite cluster is partitioned, only the side of the cluster that contains a majority of the nodes will be available. The other side of the cluster will not respond to writes. However the side that remains available will return consistent results, and when the partition is healed, consistent results will continue to be returned.

## Does the rqlite require consensus be reached before a write is accepted?
Yes, this is an intrinsic part of the Raft protocol. How long it takes to reach consensus depends primarily on your network. It will take two rounds trips from a leader to a quorum of nodes, though each of those nodes is contacted in parallel.

## How does a client detect a cluster partition?
If the client is on the same side of the partition as a quorum of nodes, there will be no real problem, and any writes should succeed. However if the client is on the other side of the partition, one of two things will happen. The client may be redirected to the leader, but will then (presumably) fail to contact the leader due to the partition, and experience a timeout. Alternatively the client may receive a `no leader` error.

It may be possible to make partitions clearer to clients in a future release.

## Can I run a single node?
Sure. Many people do so, as they like accessing a SQLite database over HTTP. 

## Is rqlite a good match for a network of nodes that come and go -- perhaps thousands of them?
Unlikely. While rqlite does support read-only nodes, allowing it to scale to many nodes, the consensus protocol at the core of rqlite works best when the nodes in the cluster don't continually come and go. While it won't break, it probably won't be practical.

## Is it a drop-in replacement for SQLite?
No. While it does use SQLite as its storage engine, you must access the system via HTTP. That said, since it basically exposes SQLite, all the power of that database is available. It is also possible that any system built on top of SQLite only needs small changes to work with rqlite.

## Can I modify the SQLite file directly?
No, you must only change the database using the HTTP API.

## Can I use rqlite to replicate my SQLite database to a second node?
Not in a simple sense, no. rqlite is not a SQLite database replication tool. While each node does have a full copy of the SQLite database, rqlite is not simply about replicating that database.

## Is the underlying serializable isolation level of SQLite maintained?
Yes, it is.

## Do concurrent writes block each other? 
In this regard rqlite currently offers exactly the same semantics as SQLite. Each HTTP write request uses the same SQLite connection on the leader, so one write-over-HTTP may block another. Explicit connection control will be available in a future release, which will clients more control over transactions. Only one concurrent write will ever be supported however, due to the nature of SQLite.

## How is it different than dqlite?
dqlite is library, written in C, that you can integrate with your own software. This requires programming. rqlite is a standalone program -- it's a full RDBMS (albeit a relatively simple one).
