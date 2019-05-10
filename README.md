<img src="DOC/logo-text.png" height=100></img>

[![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master) [![appveyor](https://ci.appveyor.com/api/projects/status/github/rqlite/rqlite?branch=master&svg=true)](https://ci.appveyor.com/project/otoolep/rqlite) [![GoDoc](https://godoc.org/github.com/rqlite/rqlite?status.svg)](https://godoc.org/github.com/rqlite/rqlite) [![Go Report Card](https://goreportcard.com/badge/github.com/rqlite/rqlite)](https://goreportcard.com/report/github.com/rqlite/rqlite) [![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases) [![Google Group](https://img.shields.io/badge/Google%20Group--blue.svg)](https://groups.google.com/group/rqlite)

*rqlite* is an easy-to-use, lightweight, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. Forming a cluster is very straightforward, it gracefully handles leader elections, and tolerates failures of machines, including the leader. rqlite is available for Linux, OSX, and Microsoft Windows.

### Why?
rqlite gives you the functionality of a [rock solid](http://www.sqlite.org/testing.html), fault-tolerant, replicated relational database, but with very **easy installation, deployment, and operation**. With it you've got a **lightweight** and **reliable distributed relational data store**. Think [etcd](https://github.com/coreos/etcd/) or [Consul](https://github.com/hashicorp/consul), but with relational data modelling also available.

You could use rqlite as part of a larger system, as a central store for some critical relational data, without having to run larger, more complex distributed databases.

### How?
rqlite uses [Raft](https://raft.github.io/) to achieve consensus across all the instances of the SQLite databases, ensuring that every change made to the system is made to a quorum of SQLite databases, or none at all. You can learn more about the design [here](https://github.com/rqlite/rqlite/blob/master/DOC/DESIGN.md).

### Key features
- Very easy deployment, with no need to separately install SQLite.
- Fully replicated production-grade SQL database.
- [Production-grade](https://github.com/hashicorp/raft) distributed consensus system.
- An easy-to-use [HTTP(S) API](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md), including leader-redirection and bulk-update support. A CLI is also available, as are various [client libraries](https://github.com/rqlite).
- [Discovery Service support](https://github.com/rqlite/rqlite/blob/master/DOC/DISCOVERY.md), allowing clusters to be dynamically created.
- [Extensive security and encryption support](https://github.com/rqlite/rqlite/blob/master/DOC/SECURITY.md), including node-to-node encryption.
- Choice of [read consistency levels](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md).
- A flavour of transaction support.
- [Hot backups](https://github.com/rqlite/rqlite/blob/master/DOC/BACKUPS.md).

## Quick Start
_Full documentation available [here](https://github.com/rqlite/rqlite/tree/master/DOC). You may also wish to check out the [rqlite Google Group](https://groups.google.com/forum/#!forum/rqlite)._

The quickest way to get running on OSX and Linux is to download a pre-built release binary. You can find these binaries on the [Github releases page](https://github.com/rqlite/rqlite/releases). If you prefer Windows you can download the latest build [here](https://ci.appveyor.com/api/projects/otoolep/rqlite/artifacts/rqlite-latest-win64.zip?branch=master). Once installed, you can start a single rqlite node like so:
```bash
rqlited ~/node.1
```
This single node automatically becomes the leader. You can pass `-h` to `rqlited` to list all configuration options.

### Docker
Alternatively you can pull the latest release via `docker pull rqlite/rqlite`. [Docker support](https://github.com/rqlite/rqlite-docker) is still very early.

### Forming a cluster
While not strictly necessary to run rqlite, running multiple nodes means you'll have a fault-tolerant cluster. Start two more nodes, allowing the cluster to tolerate failure of a single node, like so:
```bash
rqlited -http-addr localhost:4003 -raft-addr localhost:4004 -join http://localhost:4001 ~/node.2
rqlited -http-addr localhost:4005 -raft-addr localhost:4006 -join http://localhost:4001 ~/node.3
```
_This demonstration shows all 3 nodes running on the same host. In reality you probably wouldn't do this, and then you wouldn't need to select different -http-addr and -raft-addr ports for each rqlite node._

With just these few steps you've now got a fault-tolerant, distributed relational database. For full details on creating and managing real clusters check out [this documentation](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md).

#### Cluster Discovery
There is also a rqlite _Discovery Service_, allowing nodes to automatically connect and form a cluster. This can be much more convenient, allowing clusters to be dynamically created. Check out [the documentation](https://github.com/rqlite/rqlite/blob/master/DOC/DISCOVERY.md) for more details.

### Inserting records
Let's insert some records via the [rqlite CLI](https://github.com/rqlite/rqlite/blob/master/DOC/CLI.md), using standard SQL commands. Once inserted, these records will be replicated across the cluster, in a durable and fault-tolerant manner. Your 3-node cluster can suffer the failure of a single node without any loss of functionality.
```
$ rqlite
127.0.0.1:4001> CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)
0 row affected (0.000668 sec)
127.0.0.1:4001> .schema
+-----------------------------------------------------------------------------+
| sql                                                                         |
+-----------------------------------------------------------------------------+
| CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)               |
+-----------------------------------------------------------------------------+
127.0.0.1:4001> INSERT INTO foo(name) VALUES("fiona")
1 row affected (0.000080 sec)
127.0.0.1:4001> SELECT * FROM foo
+----+-------+
| id | name  |
+----+-------+
| 1  | fiona |
+----+-------+
```

## Data API
rqlite has a rich HTTP API, allowing full control over writing to, and querying from, rqlite. Check out [the documentation](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md) for full details. There are also [client libraries available](https://github.com/rqlite).

## Performance
rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine (particularly its IO performance) and network, individual INSERT performance could be anything from 10 operations per second to more than 200 operations per second. However, by using the [bulk API](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md), transactions, or both, throughput will increase significantly, often by 2 orders of magnitude. This speed-up is due to the way SQLite works. So for high throughput, execute as many operations as possible within a bulk request.

## Limitations
 * Only SQL statements that are __deterministic__ are safe to use with rqlite, because statements are committed to the Raft log before they are sent to each node. In other words, rqlite performs _statement-based replication_. For example, the following statement could result in different a SQLite database under each node:
```
INSERT INTO foo (n) VALUES(random());
```
 * Technically this is not supported, but you can directly read the SQLite under any node at anytime, assuming you run in "on-disk" mode. However there is no guarantee that the SQLite file reflects all the changes that have taken place on the cluster unless you are sure the host node itself has received and applied all changes.
 * In case it isn't obvious, rqlite does not replicate any changes made directly to any underlying SQLite file, when run in "on disk" mode. **If you change the SQLite file directly, you may cause rqlite to fail**. Only modify the database via the HTTP API.
 * SQLite dot-commands such as `.schema` or `.tables` are not directly supported by the API, but the rqlite CLI supports some very similar functionality. This is because those commands are features of the `sqlite3` command, not SQLite itself.

## Status API
You can learn how check status and diagnostics [here](https://github.com/rqlite/rqlite/blob/master/DOC/DIAGNOSTICS.md).

## Backup and restore
Learn how to hot backup your rqlite cluster [here](https://github.com/rqlite/rqlite/blob/master/DOC/BACKUPS.md). You can also load data [directly from a SQLite dump file](https://github.com/rqlite/rqlite/blob/master/DOC/RESTORE_FROM_SQLITE.md).

## Security
You can learn about securing access, and restricting users' access, to rqlite [here](https://github.com/rqlite/rqlite/blob/master/DOC/SECURITY.md).

## Google Group
There is a [Google Group](https://groups.google.com/forum/#!forum/rqlite) dedicated to discussion of rqlite.

## Pronunciation?
How do I pronounce rqlite? For what it's worth I try to pronounce it "ree-qwell-lite". But it seems most people, including me, often pronouce it "R Q lite".
