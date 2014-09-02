rqlite
======

*rqlite* is a distributed system that provides a replicated SQLite database. rqlite is written in [Go](http://golang.org/) and uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a majority of databases, or none-at-all.

### Why replicate SQLite?
[SQLite](http://www.sqlite.org/) is a "self-contained, serverless, zero-configuration, transactional SQL database engine". The entire database is contained within a single file on disk, making working with it very straightforward. Many people have experience with it, and it's been a natural choice for adding relational-database functionality to many systems. However, SQLite isn't replicated, which means it can become a single point of failure if used to store metadata about a cluster of manchines. While it is possible to continually copy the SQLite file to a backup server everytime it is changed, this file-copy must not take place while the database is being accessed.

rqlite combines the ease-of-use of SQLite with straightfoward replication. And it was fun. :-)

## Building and Running
Download and run rqlite like so (tested on 64-bit Kubuntu 14.04):

    mkdir rqlite # Or any directory of your choice.
    cd rqlite/
    export GOPATH=$PWD
    go get github.com/otoolep/rqlite
    bin/rqlite ~/node.1

This starts a rqlite server listening on localhost, port 4001. This single node automatically becomes the leader. To see all available command-line options, execute:

    bin/rqlite -h

### Forming a Cluster
While not strictly necessary to run rqlite, running multiple nodes means the SQLite database is replicated.

Start a second and third node (so a majority can still form in the event of a single node failure) like so:

    bin/rqlite -join localhost:4001 -p 4002 ~/node.2
    bin/rqlite -join localhost:4001 -p 4003 ~/node.3

Under each node will be an SQLite file, which should remain in consensus.

## Data API
rqlite exposes an HTTP API allowing the database to modified and queried. Modifications go through the Raft log, ensuring only changes committed by a quorom of Raft servers are actually executed against the SQLite database. Queries do not go through the Raft log, since they do not change the state of the database, and therefore do not need to be captured in the log.

All responses from rqlite are in the form of JSON.

### Writing Data
To write data to the database, you must create at least 1 table.

    curl -XPOST localhost:4001/db -d 'CREATE TABLE foo (id integer not null primary key, name text)'

To insert an entry into the database, execute a second command:

    curl -XPOST localhost:4001/db?pretty -d 'INSERT INTO foo(name) VALUES("fiona")'

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses. You can confirm that the data has been write to the database by accessing the SQLite database directly.

     $ sqlite3 ~/node.3/db.sqlite
    SQLite version 3.7.15.2 2013-01-09 11:53:05
    Enter ".help" for instructions
    Enter SQL statements terminated with a ";"
    sqlite> select * from foo;
    1|fiona

#### Transactions
Transactions are supported. To execute two statements within a transaction, separate them with a newline and add `transaction` to the URL. An example is shown below:

    curl -XPOST 'localhost:4001/db?pretty&transaction' -d '
                 INSERT INTO foo(name) VALUES("fiona")
                 INSERT INTO foo(name) VALUES("fiona")'

When a transaction takes place either both statements will succeed, or neither.

### Querying Data
Qeurying data is easy.

    curl -XGET localhost:4002/db -d 'SELECT * from foo'

### Performance
rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine, individual INSERT performance could be anything from 1 operation per second to more than 10 operations per second. However, by using transactions, throughput will increase significantly, often by 2 orders or magnitude. This speed-up is due to the way SQLite works. So for high throughput, execute as many commands as possible within a single transaction.

## Admin API
An Admin API exists, which dumps some basic diagnostic and statistical information. Assuming rqlite is started with default settings, the endpoints are available like so:

    curl localhost:4001/diagnostics?pretty
    curl localhost:4001/statistics?pretty

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses.

## Credits
This project uses the [go-raft](https://github.com/goraft/raft) implementation of the Raft consensus protocol, and was inspired by the [raftd](https://github.com/goraft/raftd) reference implementation. rqlite also borrows some ideas from [etcd](https://github.com/coreos/etcd).

## Limitations
 * SQLite commands such as `.schema` are not handled.
 * Using `PRAGMA` directives has not been tested either.
 * The supported types are those supported by [go-sqlite3](http://godoc.org/github.com/mattn/go-sqlite3).
