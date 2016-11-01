<img src="doc/logo-text.png" height=100></img>

[![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master) [![appveyor](https://ci.appveyor.com/api/projects/status/github/rqlite/rqlite?branch=master&svg=true)](https://ci.appveyor.com/project/otoolep/rqlite) [![GoDoc](https://godoc.org/github.com/rqlite/rqlite?status.svg)](https://godoc.org/github.com/rqlite/rqlite) [![Go Report Card](https://goreportcard.com/badge/github.com/rqlite/rqlite)](https://goreportcard.com/report/github.com/rqlite/rqlite) [![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases)

======
*rqlite* is a distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. rqlite uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases, ensuring that every change made to the system is made to a quorum of SQLite databases, or none at all. It also gracefully handles leader elections, and tolerates failures of machines, including the leader. rqlite is compatible with Linux, OSX, and Microsoft Windows.

### Why?
rqlite gives you the functionality of a [rock solid](http://www.sqlite.org/testing.html), fault-tolerant, replicated relational database, but with very **easy installation, deployment, and operation**. With it you've got a **lightweight** and **reliable distributed relational data store**. Think [etcd](https://github.com/coreos/etcd/) or [Consul](https://github.com/hashicorp/consul), but with relational data modelling also available.

You could use rqlite as part of a larger system, as a central store for some critical relational data, without having to run a heavier solution like MySQL.

### Key features
- Very easy deployment, with no need to separately install SQLite.
- Fully replicated production-grade SQL database.
- [Production-grade] (https://github.com/hashicorp/raft) distributed consensus system.
- An easy-to-use HTTP(S) API, including leader-redirection and bulk-update support. A CLI is also available.
- Basic auth security and user-level permissions.
- Read consistency levels.
- Transaction support.
- Hot backups.

## Quick Start
The quickest way to get running on OSX and Linux is to download a pre-built release binary. You can find these binaries on the [Github releases page](https://github.com/rqlite/rqlite/releases). Once installed, you can start a single rqlite node like so:
```bash
rqlited ~/node.1
```
This single node automatically becomes the leader. You can pass `-h` to `rqlited` to list all configuration options.

### Forming a cluster
While not strictly necessary to run rqlite, running multiple nodes means the SQLite database is replicated. Start a second and third node (so a majority can still form in the event of a single node failure) like so:
```bash
rqlited -http localhost:4003 -raft localhost:4004 -join http://localhost:4001 ~/node.2
rqlited -http localhost:4005 -raft localhost:4006 -join http://localhost:4001 ~/node.3
```
_This demonstration shows all 3 nodes running on the same host. In reality you probably wouldn't do this, and then you wouldn't need to select different -http and -raft ports for each rqlite node._

With just these few steps you've now got a fault-tolerant, distributed relational database. For full details on creating and managing real clusters check out [this documentation](https://github.com/rqlite/rqlite/blob/master/doc/CLUSTER_MGMT.md).

### Inserting records
Let's insert some records via the [rqlite CLI](https://github.com/rqlite/rqlite/blob/master/doc/CLI.md), using standard SQLite commands. Once inserted, these records will be replicated across the cluster, in a durable and fault-tolerant manner.
```
$ rqlite
127.0.0.1:4001> CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)
0 row affected (0.000668 sec)
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
rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API. Modifications go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually executed against the SQLite database. Queries do not __necessarily__ go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log. More on this later.

There are also [client libraries available](https://github.com/rqlite).

### Writing Data
To write data successfully to the database, you must create at least 1 table. To do this perform a HTTP POST, with a `CREATE TABLE` SQL command encapsulated in a JSON array, in the body of the request. An example via [curl](http://curl.haxx.se/):

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "CREATE TABLE foo (id integer not null primary key, name text)"
]'
```

To insert an entry into the database, execute a second SQL command:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "INSERT INTO foo(name) VALUES(\"fiona\")"
]'
```

The response is of the form:

```json
{
    "results": [
        {
            "last_insert_id": 1,
            "rows_affected": 1,
            "time": 0.00886
        }
    ],
    "time": 0.0152
}
```

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses. Time is measured in seconds. If you do not want timings, do not pass `timings` as a URL parameter.

### Querying Data
Querying data is easy. The most important thing to know is that, by default, queries must go through the leader node. More on this later.

For a single query simply perform a HTTP GET, setting the query statement as the query parameter `q`:

```bash
curl -G 'localhost:4001/db/query?pretty&timings' --data-urlencode 'q=SELECT * FROM foo'
```

The response is of the form:

```json
{
    "results": [
        {
            "columns": [
                "id",
                "name"
            ],
            "types": [
                "integer",
                "text"
            ],
            "values": [
                [
                    1,
                    "fiona"
                ]
            ],
            "time": 0.0150043
        }
    ],
    "time": 0.0220043
}
```

#### Read Consistency
You can learn all about the read consistency guarantees supported by rqlite [here](https://github.com/rqlite/rqlite/blob/master/doc/CONSISTENCY.md).

### Transactions
Transactions are supported. To execute statements within a transaction, add `transaction` to the URL. An example of the above operation executed within a transaction is shown below.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&transaction' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
```

When a transaction takes place either both statements will succeed, or neither. Performance is *much, much* better if multiple SQL INSERTs or UPDATEs are executed via a transaction. Note that processing of the request ceases the moment any single query results in an error.

The behaviour of rqlite when using `BEGIN`, `COMMIT`, or `ROLLBACK` to control transactions is **not defined**. It is important to control transactions only through the query parameters shown above.

### Handling Errors
If an error occurs while processing a statement, it will be marked as such in the response. For example.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d "[
    \"INSERT INTO nonsense\"
]"
```
```json
{
    "results": [
        {
            "error": "near \"nonsense\": syntax error"
        }
    ],
    "time": 2.478862
}
```
### Bulk API
You can learn about the bulk API [here](https://github.com/rqlite/rqlite/blob/master/doc/BULK.md).

## Performance
rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine, individual INSERT performance could be anything from 1 operation per second to more than 100 operations per second. However, by using transactions, throughput will increase significantly, often by 2 orders of magnitude. This speed-up is due to the way SQLite works. So for high throughput, execute as many operations as possible within a single transaction.

### In-memory databases
By default rqlite uses an [in-memory SQLite database](https://www.sqlite.org/inmemorydb.html) to maximise performance. In this mode no actual SQLite file is created and the entire database is stored in memory. If you wish rqlite to use an actual file-based SQLite database, pass `-ondisk` to rqlite on start-up.

#### Does using an in-memory database put my data at risk?
No.

Since the Raft log is the authoritative store for all data, and it is written to disk, an in-memory database can be fully recreated on start-up. Using an in-memory database does not put your data at risk.

## Limitations
 * Only SQL statements that are __deterministic__ are safe to use with rqlite, because statements are committed to the Raft log before they are sent to each node. For example, the following statement could result in different a SQLite database under each node:
```
INSERT INTO foo (n) VALUES(random());
```
 * Technically this is not supported, but you can directly read the SQLite under any node at anytime, assuming you run in "on-disk" mode. However there is no guarantee that the SQLite file reflects all the changes that have taken place on the cluster unless you are sure the host node itself has received and applied all changes.
 * In case it isn't obvious, rqlite does not replicate any changes made directly to any underlying SQLite files, when run in "on disk" mode. If you do change these files directly, you will cause rqlite to fail. Only modify the database via the HTTP API.
 * SQLite commands such as `.schema` are not handled.

## Status API
You can learn how check status and diagnostics [here](https://github.com/rqlite/rqlite/blob/master/doc/DIAGNOSTICS.md).

## Backups
Learn how to backup your rqlite cluster [here](https://github.com/rqlite/rqlite/blob/master/doc/BACKUPS.md).

## Security
You can learn about securing access, and restricting users' access, to rqlite [here](https://github.com/rqlite/rqlite/blob/master/doc/SECURITY.md).

## Projects using rqlite
If you are using rqlite in a project, please generate a pull request to add it to this list.
* [OpenMarket](https://github.com/openmarket/openmarket) - A free, decentralized bitcoin-based marketplace.

## Pronunciation?
How do I pronounce rqlite? For what it's worth I pronounce it "ree-qwell-lite".
