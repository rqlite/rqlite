/*
rqlite is a distributed system that provides a replicated SQLite database. rqlite is written in Go and uses Raft to achieve consensus across all the instances of the SQLite databases. rqlite ensures that every change made to the database is made to a majority of underlying SQLite files, or none at all.
*/

package rqlite
