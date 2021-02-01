#!/bin/bash

RQLITED=~/rqlite/src/github.com/rqlite/rqlite/cmd/rqlited/rqlited
RQBENCH=~/rqlite/src/github.com/rqlite/rqlite/cmd/rqbench/rqbench
DATA_DIR=`mktemp -d`

$RQLITED -raft-snap 13 -raft-snap-int=1s $DATA_DIR &
sleep 3
$RQBENCH -b 10 -m 1 -n 500 -o 'CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)' 'INSERT INTO foo(name) VALUES("fiona")'

killall rqlited
sleep 3

$RQLITED -raft-snap 50000 -raft-snap-int=500s $DATA_DIR & 
sleep 3
$RQBENCH -b 10 -m 1 -n 500 'INSERT INTO foo(name) VALUES("fiona")'
wait
