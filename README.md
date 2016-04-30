rqlite [![Circle CI](https://circleci.com/gh/rqlite/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/rqlite/rqlite/tree/master) [![GoDoc](https://godoc.org/github.com/rqlite/rqlite?status.svg)](https://godoc.org/github.com/rqlite/rqlite) [![Release](https://img.shields.io/github/release/rqlite/rqlite.svg)](https://github.com/rqlite/rqlite/releases)
======
*rqlite* is a distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. rqlite is written in [Go](http://golang.org/) and uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a quorum of SQLite databases, or none at all.

### Why?
rqlite gives you the functionality of a [rock solid](http://www.sqlite.org/testing.html), fault-tolerant, replicated relational database, but with very easy installation, deployment, and operation. With it you've got a lightweight and reliable distributed store for relational data.

You could use rqlite as part of a larger system, as a central store for some critical relational data, without having to run a heavier solution like MySQL.

### Key features
- Very easy deployment.
- Fully replicated production-grade SQL database.
- An easy-to-use HTTP(S) API, including bulk-update support.
- Basic auth security and user-level permissions.
- Read consistency levels.
- Transaction support.
- Hot backups.

## Getting started
The quickest way to get running on OSX and Linux is to download a pre-built release binary. You can find these binaries on the [Github releases page](https://github.com/rqlite/rqlite/releases). Once installed, you can start a single rqlite node like so:
```bash
rqlited ~/node.1
```
This single node automatically becomes the leader. You can pass `-h` to `rqlited` to list all configuration options.

__If you want to build rqlite__, either because you want the latest code or a pre-built binary for platform is not available, take a look at the [contributing guidelines](https://github.com/rqlite/rqlite/blob/master/CONTRIBUTING.md).

### Forming a cluster
While not strictly necessary to run rqlite, running multiple nodes means the SQLite database is replicated.

Start a second and third node (so a majority can still form in the event of a single node failure) like so:

```bash
rqlited -http localhost:4003 -raft localhost:4004 -join http://localhost:4001 ~/node.2
rqlited -http localhost:4005 -raft localhost:4006 -join http://localhost:4001 ~/node.3
```

Under each node will be an SQLite file, which should remain in consensus. You can create clusters of any size, but clusters of 3, 5, and 7 nodes are most practical. Clusters larger than this become impractical, due to the number of nodes that must be contacted before a change can take place.

When restarting a node, there is no further need to pass `-join`. It will be ignored if a node is already a member of a cluster.

## Data API
rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API, though the SQLite database could be queried directly. Modifications go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually executed against the SQLite database. Queries do not __necessarily__ go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log. More on this later.

### Writing Data
To write data successfully to the database, you must create at least 1 table. To do this, perform a HTTP POST, with a `CREATE TABLE` SQL command encapsulated in a JSON array, in the body of the request. An example via [curl](http://curl.haxx.se/):

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

### Bulk Updates
Bulk updates are supported. To execute multipe statements in one HTTP call, simply include the statements in the JSON array:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
```

The response is of the form:

```json
{
    "results": [
        {
            "last_insert_id": 1,
            "rows_affected": 1,
            "time": 0.00759015
        },
        {
            "last_insert_id": 2,
            "rows_affected": 1,
            "time": 0.00669015
        }
    ],
    "time": 0.869015
}
```

A bulk update is contained within a single Raft log entry, so the network round-trips between nodes in the cluster are amortized over the bulk update. This should result in better throughput, if it is possible to use this kind of update.

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
                ],
                [
                    2,
                    "sinead"
                ]
            ],
            "time": 0.0150043
        }
    ],
    "time": 0.0220043
}
```

The behaviour of rqlite when more than 1 query is passed via `q` is undefined. If you want to execute more than one query per HTTP request, perform a POST, and place the queries in the body of the request as a JSON array. For example:

```bash
curl -XPOST 'localhost:4001/db/query?pretty' -H "Content-Type: application/json" -d '[
    "SELECT * FROM foo",
    "SELECT * FROM bar"
]'
```

Another approach is to read the database file directly via `sqlite3`, the command-line tool that comes with SQLite. As long as you can be sure the file you access is under the leader, the records returned will be accurate and up-to-date.

**If you use the query API to execute a command that modifies the database, those changes will not be replicated**. Always use the write API for inserts and updates.

#### Read Consistency
Even though serving queries does not require consensus (because the database is not changed), [queries should generally be served by the leader](https://github.com/rqlite/rqlite/issues/5). Why is this? Because without this check queries on a node could return out-of-date results.  This could happen for one of two reasons:

 * The node, while still part of the cluster, has fallen behind the leader.
 * The node is no longer part of the cluster, and has stopped receiving Raft log updates.

This is why rqlite offers read consistency levels of _none_, _weak_, and _strong_. Each is explained below.

With _none_, the node simply queries its local SQLite file, and does not even check if it is leader. This offers the fastest query response, but suffers from the problems listed above. _Weak_ instructs the node to check that it is the leader, before querying the local SQLite file. Checking leader state only involves checking local state, so is still very fast. There is, however, a very small window of time (milliseconds by default) during which the node may return stale data. This is because after the leader check, but before the local SQLite file is read, another node could be elected leader. As result the node may not be up-to-date with the rest of cluster. To avoid even this possibility, rqlite also offers _strong_. In this mode, rqlite sends the query through Raft consensus system, ensuring that the node remains the leader throughout query processing. However, this will involve the leader contacting at least a quorum of nodes, and will therefore increase query response times.

_Weak_ is probably sufficient for most applications, and is the default read consistency level. To explicitly select consistency, set the query param `level`. Examples of enabling each read consistency level for a simple query is shown below.

```bash
curl -G 'localhost:4001/db/query?level=none' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query?level=weak' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query?level=strong' --data-urlencode 'q=SELECT * FROM foo'
```

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
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO nonsense\"
]"
```
```json
{
    "results": [
        {
            "last_insert_id": 3,
            "rows_affected": 1,
            "time": 182.033
        },
        {
            "error": "near \"nonsense\": syntax error"
        }
    ],
    "time": 2.478862
}
```

## Performance
rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine, individual INSERT performance could be anything from 1 operation per second to more than 100 operations per second. However, by using transactions, throughput will increase significantly, often by 2 orders of magnitude. This speed-up is due to the way SQLite works. So for high throughput, execute as many operations as possible within a single transaction.

### In-memory databases
By default rqlite uses an [in-memory SQLite database](https://www.sqlite.org/inmemorydb.html) to maximise performance. In this mode no actual SQLite file is created and the entire database is stored in memory. If you wish rqlite to use an actual file-based SQLite database, pass `-ondisk` to rqlite on start-up.

#### Does using an in-memory database put my data at risk?
No.

Since the Raft log is the authoritative store for all data, and it is written to disk, an in-memory database can be fully recreated on start-up. Using an in-memory database does not put your data at risk.

## Limitations
 * Only SQL statements that are __deterministic__ are safe to use with rqlite, because statements are committed to the Raft log before they are sent to each node. For example, the following statement could result in different SQLite databases under each node:
```
INSERT INTO foo (n) VALUES(random());
```
 * In case it isn't obvious, rqlite does not replicate any changes made directly to any underlying SQLite files, when run in "on disk" mode. If you do change these files directly, you will cause rqlite to fail. Only modify the database via the HTTP API.
 * SQLite commands such as `.schema` are not handled.
 * The supported types are those supported by [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3).

## Status API
You can learn how check status and diagnostics [here](https://github.com/rqlite/rqlite/blob/master/DIAGNOSTICS.md).

## Backups
Learn how to backup your rqlite cluster [here](https://github.com/rqlite/rqlite/blob/master/BACKUPS.md).

## Security
You can learn about securing access, and restricting users' access, to rqlite [here](https://github.com/rqlite/rqlite/blob/master/SECURITY.md).

## Pronunciation?
How do I pronounce rqlite? For what it's worth I pronounce it "ree-qwell-lite".
