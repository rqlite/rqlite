# FAQ

* [What exactly does rqlite do?](#what-exactly-does-rqlite-do)
* [Why would I use this, versus some other replicated database?](#why-would-i-use-this-versus-some-other-replicated-database)
* [How do I access the database?](#how-do-i-access-the-database)
* [Can any node execute a write request, and have the system "synchronize it all"?](#can-any-node-execute-a-write-request-and-have-the-system-synchronize-it-all)
* [rqlite is distributed. Does that mean it can increase SQLite performance?](#rqlite-is-distributed-does-that-mean-it-can-increase-sqlite-performance)
* [What is the best way to increase rqlite performance?](#what-is-the-best-way-to-increase-rqlite-performance)
* [Where does rqlite fit into the CAP theorem?](#where-does-rqlite-fit-into-the-cap-theorem)
* [Does rqlite require consensus be reached before a write is accepted?](#does-rqlite-require-consensus-be-reached-before-a-write-is-accepted)
* [How does a client detect a cluster partition?](#how-does-a-client-detect-a-cluster-partition)
* [Can I run a single node?](#can-i-run-a-single-node)
* [Is rqlite a good match for a network of nodes that come and go -- perhaps thousands of them?](#is-rqlite-a-good-match-for-a-network-of-nodes-that-come-and-go----perhaps-thousands-of-them)
* [Is it a drop-in replacement for SQLite?](#is-it-a-drop-in-replacement-for-sqlite)
* [Does rqlite support transactions?](#does-rqlite-support-transactions)
* [Can I modify the SQLite file directly?](#can-i-modify-the-sqlite-file-directly)
* [Can I use rqlite to replicate my SQLite database to a second node?](#can-i-use-rqlite-to-replicate-my-sqlite-database-to-a-second-node)
* [Is the underlying serializable isolation level of SQLite maintained?](#is-the-underlying-serializable-isolation-level-of-sqlite-maintained)
* [Do concurrent writes block each other?](#do-concurrent-writes-block-each-other)
* [How is it different than dqlite?](#how-is-it-different-than-dqlite)

## What exactly does rqlite do?
rqlite is about replicating a set of data, which has been written to it using SQL. The data is replicated for fault tolerance because your data is so important that you want multiple copies distributed in different places, you want be able to query your data even if some machines fail, or both. These different places could be different machines on a rack, or different machines, each in different buildings, or even different machines, each on different continents.

On top of that, rqlite provides strong guarantees about what state any copy of that data is in, with respect to a special node called the _leader_. That is where Raft comes in. It prevents divergent copies of the data, and ensures there is an "authoritative" copy of that data at all times.

## Why would I use this, versus some other replicated database?
rqlite is very simple to deploy, run, and manage. It's a single binary you can drop anywhere on a machine, and just start it. This makes it very convenient. It takes literally seconds to configure and form a cluster.

That said, it's always possible it's _too_ simple for your needs.

## How do I access the database?
The primary way to access the database is via the [HTTP API](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md). You can access it directly, or use a [client library](https://github.com/rqlite). For more casual use you can use the [command line tool](https://github.com/rqlite/rqlite/blob/master/DOC/CLI.md).

## Can any node execute a write request, and have the system "synchronize it all"?
No, only the leader can make changes to the database. A client can _send_ a write-request to any node, and if that node is not the leader, the node will respond with the address of the leader, allowing the client to resend the request to the actual leader.

## rqlite is distributed. Does that mean it can increase SQLite performance?
Yes, but only for reads. It does not provide any scaling for writes, since all writes must go through the leader. **rqlite is distributed primarily for replication and fault tolerance, not for peformance**. In fact write performance is reduced relative to a standalone SQLite database, because of the round-trips between nodes.

## What is the best way to increase rqlite performance?
The simplest way to increase performance is to use higher-performance disks and a lower-latency network. This is known as _scaling vertically_.

## Where does rqlite fit into the CAP theorem?
The [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem) states that it is impossible for a distributed database to provide consistency, availability, and partition tolerance simulataneously -- that, in the face of a network partition, the database can be available or consistent, but not both.

Raft is a Consistency-Partition (CP) protocol. This means that if a rqlite cluster is partitioned, only the side of the cluster that contains a majority of the nodes will be available. The other side of the cluster will not respond to writes. However the side that remains available will return consistent results, and when the partition is healed, consistent results will continue to be returned.

## Does rqlite require consensus be reached before a write is accepted?
Yes, this is an intrinsic part of the Raft protocol. How long it takes to reach consensus depends primarily on your network. It will take two rounds trips from a leader to a quorum of nodes, though each of those nodes is contacted in parallel.

## How does a client detect a cluster partition?
If the client is on the same side of the partition as a quorum of nodes, there will be no real problem, and any writes should succeed. However if the client is on the other side of the partition, one of two things will happen. The client may be redirected to the leader, but will then (presumably) fail to contact the leader due to the partition, and experience a timeout. Alternatively the client may receive a `no leader` error.

It may be possible to make partitions clearer to clients in a future release.

## Can I run a single node?
Sure. Many people do so, as they like accessing a SQLite database over HTTP. Of course, you won't have any redundancy or fault tolerance if you only run a single node.

## What is the maximum size of a cluster?
There is no explicit maximum cluster size. However the [practical cluster size limit is about 9 _voting nodes](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md). You can go bigger by adding [read-only nodes](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md).

## Is rqlite a good match for a network of nodes that come and go -- perhaps thousands of them?
Unlikely. While rqlite does support read-only nodes, allowing it to scale to many nodes, the consensus protocol at the core of rqlite works best when the nodes in the cluster don't continually come and go. While it won't break, it probably won't be practical.

## Is it a drop-in replacement for SQLite?
No. While it does use SQLite as its storage engine, you must access the system via HTTP. That said, since it basically exposes SQLite, all the power of that database is available. It is also possible that any system built on top of SQLite only needs small changes to work with rqlite.

## Does rqlite support transactions?
It supports [a form of transactions](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md#transactions). You can wrap a bulk update in a transaction such that all the statements in the bulk request will succeed, or none of them will. However the behaviour or rqlite is undefined if you send explicit `BEGIN`, `COMMIT`, or `ROLLBACK` statements. This is not because they won't work -- they will -- but if your node (or cluster) fails while a transaction is in progress, the system may be left in a hard-to-use state. So until rqlite can offer strict guarantees about its behaviour if it fails during a transaction, using `BEGIN`, `COMMIT`, and `ROLLBACK` is officially unsupported. Unfortunately this does mean that rqlite may not be suitable for some applications.

## Can I modify the SQLite file directly?
No, you must only change the database using the HTTP API.

## Can I use rqlite to replicate my SQLite database to a second node?
Not in a simple sense, no. rqlite is not a SQLite database replication tool. While each node does have a full copy of the SQLite database, rqlite is not simply about replicating that database.

## Is the underlying serializable isolation level of SQLite maintained?
Yes, it is.

## Do concurrent writes block each other? 
In this regard rqlite currently offers exactly the same semantics as SQLite. Each HTTP write request uses the same SQLite connection on the leader, so one write-over-HTTP may block another. Explicit connection control will be available in a future release, which will clients more control over transactions. Only one concurrent write will ever be supported however, due to the nature of SQLite.

## How is it different than dqlite?
dqlite is library, written in C, that you can integrate with your own software. This requires programming. rqlite is a standalone program -- it's a full [RDBMS](https://techterms.com/definition/rdbms) (albeit a relatively simple one). rqlite has everything you need to read and write data, and backup, maintain, and monitor the database itself.
