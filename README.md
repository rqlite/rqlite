rqlite
======

*rqlite* is a distributed system that provides a replicated SQLite database. rqlite is written in [Go](http://golang.org/) and uses [Raft](http://raftconsensus.github.io/) to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a majority of databases, or none-at-all.

## Why replicate SQLite?
SQLite is a "self-contained, serverless, zero-configuration, transactional SQL database engine". The entire database is contained within a single file on disk, making working with it very straightforward. Many people have experience with it, and it's been a natural choice for adding relational-database functionality to many systems. However, SQLite isn't replicated, which means it can become a single point of failure if used to store metadata about cluster of manchines. While it is possible to continually copy the SQLite file to a backup server, this copy must not take place while the database is being accessed.

rqlite combines the ease-of-use of SQLite with straightfoward replication.

## Writing Data

## Querying Data

## Performance

Depending on your machine, individual INSERT performance could be anything from 1 operation per second to more than 10 operations per second. However, by using transactions, throughput will increase significantly, often by 2 orders or magnitude. This speed-up is due to the way SQLite works.
