<img src="DOC/logo-text.png" height=100></img>

[![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master) [![appveyor](https://ci.appveyor.com/api/projects/status/github/rqlite/rqlite?branch=master&svg=true)](https://ci.appveyor.com/project/otoolep/rqlite) [![Go Report Card](https://goreportcard.com/badge/github.com/rqlite/rqlite)](https://goreportcard.com/report/github.com/rqlite/rqlite) [![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases) [![Docker](https://img.shields.io/docker/pulls/rqlite/rqlite?style=plastic)](https://hub.docker.com/r/rqlite/rqlite/) [![Google Group](https://img.shields.io/badge/Google%20Group--blue.svg)](https://groups.google.com/group/rqlite) [![Slack](https://img.shields.io/badge/Slack--purple.svg)](https://www.philipotoole.com/join-rqlite-slack)

_When you absolutely must not lose any of your data._

*rqlite* is an easy-to-use, lightweight, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. rqlite is simple to deploy, operating it is very straightforward, and its clustering capabilities provide you with fault-tolerance and high-availability. [rqlite is available for Linux, macOS, and Microsoft Windows](https://github.com/rqlite/rqlite/releases).

_Check out the [rqlite FAQ](https://github.com/rqlite/rqlite/blob/master/DOC/FAQ.md)._

### Why?
rqlite gives you the functionality of a [rock solid](https://www.sqlite.org/testing.html), fault-tolerant, replicated relational database, but with very **easy installation, deployment, and operation**. With it you've got a **lightweight** and **reliable distributed relational data store**. Think [etcd](https://github.com/coreos/etcd/) or [Consul](https://github.com/hashicorp/consul), but with relational data modelling also available.

You could use rqlite as part of a larger system, as a central store for some critical relational data, without having to run larger, more complex distributed databases.

Finally, if you're interested in understanding how distributed systems actually work, **rqlite is a good example to study**. Much thought has gone into its [design](https://github.com/rqlite/rqlite/blob/master/DOC/DESIGN.md) and implementation, with clear separation between the various components, including storage, distributed consensus, and API.

### How?
rqlite uses [Raft](https://raft.github.io/) to achieve consensus across all the instances of the SQLite databases, ensuring that every change made to the system is made to a quorum of SQLite databases, or none at all. You can learn more about the design [here](https://github.com/rqlite/rqlite/blob/master/DOC/DESIGN.md).

### Key features
- Trivially easy to deploy, with no need to separately install SQLite.
- Super-simple to use, with a straightforward [HTTP API](https://github.com/rqlite/rqlite/blob/master/DOC/DATA_API.md). A [command-line interface is also available](https://github.com/rqlite/rqlite/tree/master/cmd/rqlite), as are various [client libraries](https://github.com/rqlite).
- Fully replicated production-grade SQL database, with full access to SQLite [full-text search](https://www.sqlite.org/fts3.html) and [JSON document support](https://www.sqlite.org/json1.html).
- Multiple options for [node-discovery and automatic clustering, including integration with Kubernetes, Consul, etcd and DNS](https://github.com/rqlite/rqlite/blob/master/DOC/AUTO_CLUSTERING.md), allowing clusters to be dynamically created.
- [Extensive security and encryption support](https://github.com/rqlite/rqlite/blob/master/DOC/SECURITY.md), including node-to-node encryption.
- Choice of [read consistency levels](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md), and support for choosing [write performance over durability](https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md).
- Optional [read-only (non-voting) nodes](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md), which can add read scalability to the system.
- A form of transaction support.
- Hot [backups](https://github.com/rqlite/rqlite/blob/master/DOC/BACKUPS.md), as well as [load directly from SQLite](https://github.com/rqlite/rqlite/blob/master/DOC/RESTORE_FROM_SQLITE.md).

## Quick Start
_Detailed documentation [is available](https://github.com/rqlite/rqlite/tree/master/DOC). Check out the [rqlite Google Group](https://groups.google.com/forum/#!forum/rqlite) and join the [rqlite Slack channel](https://www.philipotoole.com/join-rqlite-slack)_.

The quickest way to get running is to download a pre-built release binary, available on the [Github releases page](https://github.com/rqlite/rqlite/releases). Once installed, you can start a single rqlite node like so:
```bash
rqlited -node-id 1 ~/node.1
```
This single node automatically becomes the leader. You can pass `-h` to `rqlited` to list all configuration options.

### Docker
`docker run -p4001:4001 rqlite/rqlite`

Check out the [rqlite Docker page](https://hub.docker.com/r/rqlite/rqlite/) for more details on running nodes via Docker.

### Homebrew
`brew install rqlite`

### Forming a cluster
While not strictly necessary to run rqlite, running multiple nodes means you'll have a fault-tolerant cluster. Start two more nodes, allowing the cluster to tolerate failure of a single node, like so:
```bash
rqlited -node-id 2 -http-addr localhost:4003 -raft-addr localhost:4004 -join http://localhost:4001 ~/node.2
rqlited -node-id 3 -http-addr localhost:4005 -raft-addr localhost:4006 -join http://localhost:4001 ~/node.3
```
_This demonstration shows all 3 nodes running on the same host. In reality you probably wouldn't do this, and then you wouldn't need to select different -http-addr and -raft-addr ports for each rqlite node._

With just these few steps you've now got a fault-tolerant, distributed relational database. For full details on creating and managing real clusters, including running read-only nodes, check out [this documentation](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md).

#### Node Discovery and Automatic Clustering
rqlite can use [Consul](https://www.consul.io/), [etcd](https://etcd.io/), and DNS, for node discovery. This allows nodes to automatically connect and form a cluster. This can be much more convenient, allowing clusters to be dynamically created. Check out [the documentation](https://github.com/rqlite/rqlite/blob/master/DOC/AUTO_CLUSTERING.md) for more details.

#### Kubernetes
Check out the [Kubernetes](https://github.com/rqlite/rqlite/blob/master/DOC/KUBERNETES.md) deployment guide.

### Inserting records
Let's insert some records via the [rqlite CLI](https://github.com/rqlite/rqlite/blob/master/DOC/CLI.md), using standard SQLite commands. Once inserted, these records will be replicated across the cluster, in a durable and fault-tolerant manner. Your 3-node cluster can suffer the failure of a single node without any loss of functionality or data.
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
You can learn more about rqlite performance, and how to improve it, [here](https://github.com/rqlite/rqlite/blob/master/DOC/PERFORMANCE.md).

### In-memory databases
By default rqlite uses an [in-memory SQLite database](https://www.sqlite.org/inmemorydb.html) to maximise performance. In this mode no actual SQLite file is created and the entire database is stored in memory. If you wish rqlite to use an actual file-based SQLite database, pass `-on-disk` to rqlite on start-up.

#### Does using an in-memory database put my data at risk?
No.

Since the Raft log is the authoritative store for all data, and it is stored on disk by each node, an in-memory database can be fully recreated on start-up from the information stored in the Raft log. Using an in-memory database does not put your data at risk.

## Limitations
 * In-memory databases are currently limited to 2GiB (2147483648 bytes) in size. You can learn more about possible ways to get around this limit in the [documentation](https://github.com/rqlite/rqlite/blob/master/DOC/PERFORMANCE.md#in-memory-database-limits).

 * Because rqlite peforms _statement-based replication_ certain [_non-deterministic functions_](https://www.sqlite.org/deterministic.html), e.g. `RANDOM()`, are rewritten by rqlite before being passed to the Raft system and SQLite. To learn more about rqlite's support for non-deterministic functions, check out the [documentation](https://github.com/rqlite/rqlite/blob/master/DOC/NON_DETERMINISTIC_FUNCTIONS.md).

 * This has not been extensively tested, but you can directly read the SQLite file under any node at anytime, assuming you run in "on-disk" mode. However there is no guarantee that the SQLite file reflects all the changes that have taken place on the cluster unless you are sure the host node itself has received and applied all changes.
 * In case it isn't obvious, rqlite does not replicate any changes made directly to any underlying SQLite file, when run in "on disk" mode. **If you change the SQLite file directly, you may cause rqlite to fail**. Only modify the database via the HTTP API.
 * SQLite dot-commands such as `.schema` or `.tables` are not directly supported by the API, but the [rqlite CLI](https://github.com/rqlite/rqlite/blob/master/DOC/CLI.md) supports some very similar functionality. This is because those commands are features of the `sqlite3` command, not SQLite itself.

## Monitoring rqlite
For reliable operation, particularly in production, it's important to monitor rqlite. You can learn how to check rqlite status and diagnostics [here](https://github.com/rqlite/rqlite/blob/master/DOC/DIAGNOSTICS.md).

## Backup and restore
Learn how to hot backup your rqlite cluster [here](https://github.com/rqlite/rqlite/blob/master/DOC/BACKUPS.md). You can also load data [directly from a SQLite file](https://github.com/rqlite/rqlite/blob/master/DOC/RESTORE_FROM_SQLITE.md).

## Security
You can learn about securing access, and restricting users' access, to rqlite [here](https://github.com/rqlite/rqlite/blob/master/DOC/SECURITY.md).

## rqlite Slack Channel
Join the [Slack channel](https://www.philipotoole.com/join-rqlite-slack) to learn more about rqlite.

## Google Group
There is a [Google Group](https://groups.google.com/forum/#!forum/rqlite) dedicated to discussion of rqlite.

## Pronunciation?
How do I pronounce rqlite? For what it's worth I try to pronounce it "ree-qwell-lite". But it seems most people, including me, often pronouce it "R Q lite".
