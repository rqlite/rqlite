rqlite [![Circle CI](https://circleci.com/gh/otoolep/rqlite/tree/master.svg?style=svg)](https://circleci.com/gh/otoolep/rqlite/tree/master)
======

*Detailed background on rqlite can be found on [this blog post](http://www.philipotoole.com/replicating-sqlite-using-raft-consensus).*

*rqlite* is a distributed system that provides a replicated SQLite database. rqlite is written in [Go](http://golang.org/) and uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a majority of underlying SQLite files, or none-at-all.

## Building and Running
Download, test (optional), and run rqlite like so (tested on 64-bit Kubuntu 14.04):

    mkdir rqlite # Or any directory of your choice.
    cd rqlite/
    export GOPATH=$PWD
    go get github.com/otoolep/rqlite
    go get gopkg.in/check.v1; go test github.com/otoolep/rqlite/... # Optional testing
    $GOPATH/bin/rqlite ~/node.1

This starts a rqlite server listening on localhost, port 4001. This single node automatically becomes the leader. To see all available command-line options, execute:

    $GOPATH/bin/rqlite -h

### Forming a Cluster
While not strictly necessary to run rqlite, running multiple nodes means the SQLite database is replicated.

Start a second and third node (so a majority can still form in the event of a single node failure) like so:

    $GOPATH/bin/rqlite -join localhost:4001 -p 4002 ~/node.2
    $GOPATH/bin/rqlite -join localhost:4001 -p 4003 ~/node.3

Under each node will be an SQLite file, which should remain in consensus.

### Restarting a node
If a node needs to be restarted, perhaps because of failure, don't pass the `-join` option. Using the example nodes above, if node 2 needed to be restarted, do so as follows:

    $GOPATH/bin/rqlite -p 4002 ~/node.2

On restart it will rejoin the cluster and apply any changes to its local sqlite database that took place while it was down.

## Data API
rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API,though the SQLite database could be queried directly. Modifications go through the Raft log, ensuring only changes committed by a quorum of Raft servers are actually executed against the SQLite database. Queries do not go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log.

All responses from rqlite are in the form of JSON.

### Writing Data
To write data successfully to the database, you must create at least 1 table. To do this, perform a HTTP POST, with a CREATE TABLE SQL command in the body of the request. For example:

    curl -L -XPOST localhost:4001/db -d 'CREATE TABLE foo (id integer not null primary key, name text)'

where `curl` is the [well known command-line tool](http://curl.haxx.se/). Passing `-L` to `curl` ensures the command will follow any redirect (HTTP status code 307) to the leader, if the node running on port 4001 is not the leader.

To insert an entry into the database, execute a second SQL command:

    curl -L -XPOST localhost:4001/db?pretty -d 'INSERT INTO foo(name) VALUES("fiona")'

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses. You can confirm that the data has been writen to the database by accessing the SQLite database directly.

     $ sqlite3 ~/node.3/db.sqlite
    SQLite version 3.7.15.2 2013-01-09 11:53:05
    Enter ".help" for instructions
    Enter SQL statements terminated with a ";"
    sqlite> select * from foo;
    1|fiona
Note that this is the SQLite file that is under `node 3`, which is not the node that accepted the `INSERT` operation.

### Bulk Updates
Bulk updates are supported. To execute multipe statements in one HTTP call, separate each statement with a newline. An example of inserting two records is shown below:

    curl -L -XPOST 'localhost:4001/db?pretty' -d '
                 INSERT INTO foo(name) VALUES("fiona")
                 INSERT INTO foo(name) VALUES("fiona")'
#### Transactions
Transactions are supported. To execute statements within a transaction, add `transaction` to the URL. An example of the above operation executed within a transaction is shown below.

    curl -L -XPOST 'localhost:4001/db?pretty&transaction' -d '
                 INSERT INTO foo(name) VALUES("fiona")
                 INSERT INTO foo(name) VALUES("fiona")'

When a transaction takes place either both statements will succeed, or neither. Performance is *much, much* better if multiple SQL INSERTs or UPDATEs are executed via a transaction.

### Querying Data
Qeurying data is easy. Simply perform a HTTP GET with the SQL query in the body of the request.

    curl -L -XGET localhost:4001/db -d 'SELECT * from foo'
    
An alternative approach is to read the database via `sqlite3`, the command-line tool that comes with SQLite. As long as you can be sure the file you access is under the leader, the records returned will be accurate and up-to-date.

### Performance
rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine, individual INSERT performance could be anything from 1 operation per second to more than 10 operations per second. However, by using transactions, throughput will increase significantly, often by 2 orders of magnitude. This speed-up is due to the way SQLite works. So for high throughput, execute as many operations as possible within a single transaction.

## Administration API
An Administration API exists, which dumps some basic diagnostic and statistical information, as well as basic information about the underlying Raft node. Assuming rqlite is started with default settings, the endpoints are available like so:

    curl localhost:4001/raft?pretty
    curl localhost:4001/diagnostics?pretty
    curl localhost:4001/statistics?pretty

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

## Log Compaction
rqlite does perform log compaction. After a configurable number of changes to the log, rqlite snapshots the SQLite database. And at start-up rqlite loads any existing snapshot.

Review [issue #14](https://github.com/otoolep/rqlite/issues/14) to learn more about how snapshots affect node restart time.

## Limitations
 * SQLite commands such as `.schema` are not handled.
 * Using `PRAGMA` directives has not been tested either.
 * The supported types are those supported by [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3).

This is new software, so it goes without saying it has bugs. It's by no means finished -- issues are now being tracked, and I plan to develop this project further. Pull requests are also welcome.

## Credits
This project uses the [go-raft](https://github.com/goraft/raft) implementation of the Raft consensus protocol, and was inspired by the [raftd](https://github.com/goraft/raftd) reference implementation. rqlite also borrows some ideas from [etcd](https://github.com/coreos/etcd), and uses [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3) to talk to the SQLite database.
