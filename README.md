rqlite [![Circle CI](https://circleci.com/gh/otoolep/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/otoolep/rqlite/tree/master) [![GoDoc](https://godoc.org/github.com/otoolep/rqlite?status.png)](https://godoc.org/github.com/otoolep/rqlite)
======

*Detailed background on rqlite can be found on [this blog post](http://www.philipotoole.com/replicating-sqlite-using-raft-consensus). Note that master represents 2.0 development (which is still in progress), with a new API and Raft consensus module. If you want to work with 1.0 rqlite, you can find it [here](https://github.com/otoolep/rqlite/releases/tag/v1.0).*

*rqlite* is a distributed system that provides a replicated SQLite database. rqlite is written in [Go](http://golang.org/) and uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a quorum of SQLite files, or none at all.

rqlite gives you the functionality of a fault-tolerant, replicated relational database, but with very easy installation, deployment, and operation.

## Building and Running
*Building rqlite requires Go 1.3 or later. [gvm](https://github.com/moovweb/gvm) is a great tool for managing your version of Go.*

Download, test (optional), and run rqlite like so (tested on 64-bit Kubuntu 14.04):

    mkdir rqlite # Or any directory of your choice.
    cd rqlite/
    export GOPATH=$PWD
    go get -t github.com/otoolep/rqlite/...
    $GOPATH/bin/rqlited ~/node.1

This starts a rqlite server listening on localhost, port 4001. This single node automatically becomes the leader. To see all available command-line options, execute:

    $GOPATH/bin/rqlited -h

### Forming a Cluster
While not strictly necessary to run rqlite, running multiple nodes means the SQLite database is replicated.

Start a second and third node (so a majority can still form in the event of a single node failure) like so:

    $GOPATH/bin/rqlited -http localhost:4003  -raft :4004 -join :4001 ~/node.2
    $GOPATH/bin/rqlited -http localhost:4005  -raft :4006 -join :4001 ~/node.3

Under each node will be an SQLite file, which should remain in consensus.

### Restarting a node
If a node needs to be restarted, perhaps because of failure, don't pass the `-join` option. Using the example nodes above, if node 2 needed to be restarted, do so as follows:

    $GOPATH/bin/rqlited -http localhost:4005 -raft :4006 ~/node.3

On restart it will rejoin the cluster and apply any changes to the local sqlite database that took place while it was down. Depending on the number of changes in the Raft log, restarts may take a little while.

## Data API
rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API, though the SQLite database could be queried directly. Modifications go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually executed against the SQLite database. Queries do not go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log.

All responses from rqlite are in the form of JSON.

### Writing Data
To write data successfully to the database, you must create at least 1 table. To do this, perform a HTTP POST, with a `CREATE TABLE` SQL command encapsulated in a JSON array, in the body of the request. For example:

    curl -L -XPOST localhost:4001/db?pretty -H "Content-Type: application/json"  -d '[
        "CREATE TABLE foo (id integer not null primary key, name text)"
    ]'

where `curl` is the [well known command-line tool](http://curl.haxx.se/). It is *very important* to set the `Content-Type` header as shown.

To insert an entry into the database, execute a second SQL command:

    curl -L -XPOST 'localhost:4001/db?pretty' -H "Content-Type: application/json" -d '[
        "INSERT INTO foo(name) VALUES(\"fiona\")"
    ]'

The response is of the form:

    {
        "results": [
            {
                "last_insert_id": 1,
                "rows_affected": 1,
                "time": "554.886µs"
            }
        ],
        "time": "2.520152ms"
    }

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

You can confirm that the data has been writen to the database by accessing the SQLite database directly.

     $ sqlite3 ~/node.3/db.sqlite
    SQLite version 3.7.15.2 2013-01-09 11:53:05
    Enter ".help" for instructions
    Enter SQL statements terminated with a ";"
    sqlite> select * from foo;
    1|fiona

Note that this is the SQLite file that is under `node 3`, which is not the node that accepted the `INSERT` operation.

### Bulk Updates
Bulk updates are supported. To execute multipe statements in one HTTP call, simply include the statements in the JSON array:

    curl -L -XPOST 'localhost:4001/db?pretty' -H "Content-Type: application/json" -d '[
        "INSERT INTO foo(name) VALUES(\"fiona\")",
        "INSERT INTO foo(name) VALUES(\"sinead\")"
    ]'

The response is of the form:

    {
        "results": [
            {
                "last_insert_id": 1,
                "rows_affected": 1,
                "time": "1.759015ms"
            },
            {
                "last_insert_id": 2,
                "rows_affected": 1,
                "time": "1.000015ms"
            }
        ],
        "time": "2.759015ms"
    }

A bulk update is contained within a single Raft log entry, so the network round-trips between nodes in the cluster are amortized over the bulk update. This should result in better throughput, if it is possible to use this kind of update.

### Querying Data
Querying data is easy.

For a single query simply perform a HTTP GET, setting the query statement as the query parameter `q`:

    curl -L -G localhost:4001/db?pretty --data-urlencode 'q=SELECT * FROM foo'

The response is of the form:

    {
        "results": [
            {
                "columns": [
                    "id",
                    "name"
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
                "time": "150.043µs"
            }
        ],
        "time": "182.033µs"
    }

The behaviour of rqlite when more than 1 query is passed via `q` is undefined. If you want to execute more than one query per HTTP request, place the queries in the body of the request, as a JSON array. For example:

    curl -L -G 'localhost:4001/db?pretty' -H "Content-Type: application/json" -d '[
        "SELECT * FROM foo",
        "SELECT * FROM bar"
    ]'

If queries are present in both the URL and the body of the request, the URL query takes precedence.

Another approach is to read the database file directly via `sqlite3`, the command-line tool that comes with SQLite. As long as you can be sure the file you access is under the leader, the records returned will be accurate and up-to-date.

*If you use the query API to execute a command that modifies the database, those changes will not be replicated*. Always use the write API for inserts and updates.

### Transactions
Transactions are supported. To execute statements within a transaction, add `transaction` to the URL. An example of the above operation executed within a transaction is shown below.

    curl -L -XPOST 'localhost:4001/db?pretty&transaction' -H "Content-Type: application/json" -d '[
        "INSERT INTO foo(name) VALUES(\"fiona\")",
        "INSERT INTO foo(name) VALUES(\"sinead\")""
    ]'

When a transaction takes place either both statements will succeed, or neither. Performance is *much, much* better if multiple SQL INSERTs or UPDATEs are executed via a transaction. Note the execution ceases the moment any single query results in an error.

### Handling Errors
If an error occurs while processing a statement, it will be marked as such in the response. For example.

    curl -L -XPOST 'localhost:4001/db?pretty' -H "Content-Type: application/json" -d '[
        "INSERT INTO foo(name) VALUES(\"fiona\")",
        "INSERT INTO nonsense"
    ]'
    {
        "results": [
            {
                "last_insert_id": 3,
                "rows_affected": 1,
                "time": "182.033µs"
            },
            {
                "error": "near \"nonsense\": syntax error"
            }
        ],
        "time": "2.478862ms"
    }

## Performance
rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine, individual INSERT performance could be anything from 1 operation per second to more than 100 operations per second. However, by using transactions, throughput will increase significantly, often by 2 orders of magnitude. This speed-up is due to the way SQLite works. So for high throughput, execute as many operations as possible within a single transaction.

### In-memory databases
You can also try using an [in-memory database](https://www.sqlite.org/inmemorydb.html). In this mode no actual SQLite file is created and the entire database is stored in memory. Using an in-memory *will not put your data at risk*. Since the Raft log is the authoritave store for all data, and is written to disk, an in-memory database can be fully recreated on start-up.

Pass `-mem` to `rqlited` at start-up to enable an in-memory database.

## Administration API
*Being refactored for v2.0, and is currently non-functional.*

An Administration API exists, which dumps some basic diagnostic and statistical information, as well as basic information about the underlying Raft node. Assuming rqlite is started with default settings, the endpoints are available like so:

    curl localhost:4001/raft?pretty
    curl localhost:4001/diagnostics?pretty
    curl localhost:4001/statistics?pretty

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

## Log Compaction
rqlite does perform log compaction. After a fixed number of changes rqlite snapshots the SQLite database, and truncates the Raft log.

## Limitations
 * SQLite commands such as `.schema` are not handled.
 * Using `PRAGMA` directives has not been tested either.
 * The supported types are those supported by [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3).

This is new software, so it goes without saying it has bugs. It's by no means finished -- issues are now being tracked, and I plan to develop this project further. Pull requests are also welcome.

## Reporting
rqlite reports a small amount anonymous data to [Loggly](http://www.loggly.com), each time it is launched. This data is just the host operating system and system architecture and is only used to track the number of rqlite deployments. Reporting can be disabled by passing `-noreport=true` to rqlite at launch time.

## Pronunciation?
How do I pronounce rqlite? For what it's worth I pronounce it "ree-qwell-lite".

## Credits
This project uses the [Hashicorp](https://github.com/hashicorp/raft) implementation of the Raft consensus protocol, and was inspired by the [raftd](https://github.com/goraft/raftd) reference implementation. rqlite also uses [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3) to talk to the SQLite database.
