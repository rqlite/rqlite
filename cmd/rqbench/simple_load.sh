#!/bin/bash

EXECUTE_HOST=localhost:4001

$RQBENCH -o 'CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, surname TEXT)' -m 100 -n 1500000 -a $EXECUTE_HOST 'INSERT INTO foo(name) VALUES("fiona")' &
$RQBENCH -o 'CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, surname TEXT)' -m 100 -n 1500000 -a $EXECUTE_HOST 'INSERT INTO bar(name, surname) VALUES("fiona", "OTOOLE")' &
$RQBENCH -o 'CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)' -m 100 -n 1500000 -a $EXECUTE_HOST 'INSERT INTO qux(name) VALUES("fionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafiona")' &

$RQBENCH -p "/db/query" -n 150000000 -m 1000 "SELECT COUNT(*) FROM foo" &
$RQBENCH -p "/db/query" -n 150000000 -m 1000 "SELECT COUNT(*) FROM bar" &
$RQBENCH -p "/db/query" -n 150000000 -m 1000 "SELECT * FROM qux LIMIT 10" &

wait
