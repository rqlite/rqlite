rqlite [![GoDoc](https://godoc.org/github.com/otoolep/rqlite?status.svg)](https://godoc.org/github.com/otoolep/rqlite)
======

*Detailed background on rqlite can be found on [these blog posts](http://www.philipotoole.com/tag/rqlite/). Note that master represents 2.0 development (which is still in progress), with a new API and Raft consensus module. If you want to work with 1.0 rqlite, you can find it [here](https://github.com/otoolep/rqlite/releases/tag/v1.0).*

*rqlite* is a distributed system that provides a replicated SQLite database. rqlite is written in [Go](http://golang.org/) and uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a quorum of SQLite files, or none at all.

rqlite gives you the functionality of a fault-tolerant, replicated relational database, but with very easy installation, deployment, and operation.

## Building and Running
*Building rqlite requires Go 1.4 or later. [gvm](https://github.com/moovweb/gvm) is a great tool for managing your version of Go.*

Download and run rqlite like so (tested on 64-bit Kubuntu 14.04 and OSX):

```bash
mkdir rqlite # Or any directory of your choice.
cd rqlite/
export GOPATH=$PWD
go get -t github.com/otoolep/rqlite/...
$GOPATH/bin/rqlited ~/node.1
```

This starts a rqlite server listening on localhost, port 4001. This single node automatically becomes the leader. To see all available command-line options, execute:

```bash
$GOPATH/bin/rqlited -h
```

### Forming a Cluster
While not strictly necessary to run rqlite, running multiple nodes means the SQLite database is replicated.

Start a second and third node (so a majority can still form in the event of a single node failure) like so:

```bash
$GOPATH/bin/rqlited -http localhost:4003  -raft :4004 -join :4001 ~/node.2
$GOPATH/bin/rqlited -http localhost:4005  -raft :4006 -join :4001 ~/node.3
```

*(This assumes you've set `GOPATH` as in the above section.)*

Under each node will be an SQLite file, which should remain in consensus. You can create clusters of any size, but clusters of 3, 5, and 7 nodes are most practical.

### Restarting a node
If a node needs to be restarted, perhaps because of failure, don't pass the `-join` option. Using the example nodes above, if node 2 needed to be restarted, do so as follows:

```bash
$GOPATH/bin/rqlited -http localhost:4005 -raft :4006 ~/node.3
```

On restart it will rejoin the cluster and apply any changes to the local sqlite database that took place while it was down. Depending on the number of changes in the Raft log, restarts may take a little while.

### Vagrant
Alternatively you can use a [Vagrant](https://www.vagrantup.com/) environment. To do so, simply [install Vagrant](https://docs.vagrantup.com/v2/installation/index.html) on your machine, a               virtualization system such as VirtualBox, and execute the following commands:

```bash
$ cd $GOPATH/src/github.com/otoolep/rqlite
$ CLUSTER_SIZE=3 vagrant up rqlite
```

This will start a Vagrant box and install rqlite with all required dependencies. This will form a cluster with `CLUSTER_SIZE` nodes.

To execute queries against the cluster you can either ssh directly to the Vagrant box via `vagrant ssh rqlite` or execute the commands directly from your local box, accessing the cluster at `192.168.   200.10` IP and any port within a range `[4001, 4001 + CLUSTER_SIZE -1]`.

To terminate the Vagrant box simply execute:

```bash
$ vagrant destroy rqlite
```

## Data API
rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API, though the SQLite database could be queried directly. Modifications go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually executed against the SQLite database. Queries do not go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log.

All responses from rqlite are in the form of JSON.

### Writing Data
To write data successfully to the database, you must create at least 1 table. To do this, perform a HTTP POST, with a `CREATE TABLE` SQL command encapsulated in a JSON array, in the body of the request. For example:

```bash
curl -XPOST localhost:4001/db/execute?pretty -H "Content-Type: application/json" -d '[
    "CREATE TABLE foo (id integer not null primary key, name text)"
]'
```

where `curl` is the [well known command-line tool](http://curl.haxx.se/).

To insert an entry into the database, execute a second SQL command:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty' -H "Content-Type: application/json" -d '[
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

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses. Time is measured in seconds.

You can confirm that the data has been writen to the database by accessing the SQLite database directly.

```bash
$ sqlite3 ~/node.3/db.sqlite
SQLite version 3.7.15.2 2013-01-09 11:53:05
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> select * from foo;
1|fiona
```

Note that this is the SQLite file that is under `node 3`, which is not the node that accepted the `INSERT` operation.

### Bulk Updates
Bulk updates are supported. To execute multipe statements in one HTTP call, simply include the statements in the JSON array:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty' -H "Content-Type: application/json" -d "[
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
curl -G localhost:4001/db/query?pretty --data-urlencode 'q=SELECT * FROM foo'
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

#### Why must queries go through the leader?
*See [issue 5](https://github.com/otoolep/rqlite/issues/5) for more discussion of this.*

Since queries do not involve consensus, why must they served by the leader? This is because without this check queries on a node could return out-of-date results.  This could happen for one of two reasons:

 * The node, which still part of the cluster, has fallen behind the leader.
 * The node is no longer part of the cluster, and has stopped receiving Raft log updates.

This is why, even though queries do not involve consensus, they must be processed by the leader. If you wish to disable the leader check, and let queries be served regardless of leader state, add `noleader` to the URL. For example:

```bash
curl -G 'localhost:4001/db/query?pretty&noleader' --data-urlencode 'q=SELECT * FROM foo'
```

Due to the way rqlite works, there is a very small window (milliseconds) where a node has been disposed as leader, but has not yet changed its internal state. Therefore, even with the leader check in place, there is a very small window of time where out-of-date results could be returned.

### Transactions
Transactions are supported. To execute statements within a transaction, add `transaction` to the URL. An example of the above operation executed within a transaction is shown below.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&transaction' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
```

When a transaction takes place either both statements will succeed, or neither. Performance is *much, much* better if multiple SQL INSERTs or UPDATEs are executed via a transaction. Note the execution ceases the moment any single query results in an error.

The behaviour of rqlite when using `BEGIN`, `COMMIT`, or `ROLLBACK` to control transactions is **not defined**. Control transactions only through the query parameters shown above.

### Handling Errors
If an error occurs while processing a statement, it will be marked as such in the response. For example.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty' -H "Content-Type: application/json" -d "[
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
You can also try using an [in-memory database](https://www.sqlite.org/inmemorydb.html) to increase performance. In this mode no actual SQLite file is created and the entire database is stored in memory.

#### Will this put my data at risk?
No.

Using an in-memory does not put your data at risk. Since the Raft log is the authoritative store for all data, and it is written to disk, an in-memory database can be fully recreated on start-up.

Pass `-mem` to `rqlited` at start-up to enable an in-memory database.

## Status API
A status API exists, which dumps some basic diagnostic and statistical information, as well as basic information about the underlying Raft node. Assuming rqlite is started with default settings, rqlite status is available like so:

```bash
curl localhost:4001/status?pretty
```

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

## Backups
rqlite supports hot-backing up a node as follows. Retrieve and write a consistent snapshot of the underlying SQLite database to a file like so:

```bash
curl localhost:4001/db/backup -o bak.sqlite3
```

The node can then be restored by loading this database file via `sqlite3` and executing `.dump`. You can then use the output of the dump to replay the entire database back into brand new node (or cluster), *with the exception* of `BEGIN TRANSACTION` and `COMMIT` commands. You should ignore those commands in the `.dump` output.

By default a backup can only be retrieved from the leader, though this check can be disabled by adding `noleader` to the URL as a query param.

## Log Compaction
rqlite does perform log compaction. After a fixed number of changes rqlite snapshots the SQLite database, and truncates the Raft log.

## Limitations
 * SQLite commands such as `.schema` are not handled.
 * The supported types are those supported by [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3).

This is new software, so it goes without saying it has bugs. It's by no means finished -- issues are now being tracked, and I plan to develop this project further. Pull requests are also welcome.

## Pronunciation?
How do I pronounce rqlite? For what it's worth I pronounce it "ree-qwell-lite".

## Credits
This project uses the [Hashicorp](https://github.com/hashicorp/raft) implementation of the Raft consensus protocol, and was inspired by the [raftd](https://github.com/goraft/raftd) reference implementation. rqlite also uses [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3) to talk to the SQLite database.
