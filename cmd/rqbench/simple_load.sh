#!/bin/bash

$RQBENCH -o 'CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, surname TEXT)' -m 100 -n 1500000 'INSERT INTO foo(name) VALUES("fiona")' &
$RQBENCH -o 'CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, surname TEXT)' -m 100 -n 1500000 'INSERT INTO bar(name, surname) VALUES("fiona", "OTOOLE")' &
$RQBENCH -o 'CREATE TABLE qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)' -m 100 -n 1500000 'INSERT INTO qux(name) VALUES("fionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafiona")' &

sleep 60
$RQBENCH -p "/db/query" -n 15000000 -m 1000 "SELECT COUNT(*) FROM foo" &
sleep 60 
$RQBENCH -p "/db/query" -n 15000000 -m 1000 "SELECT COUNT(*) FROM bar" &
sleep 60
$RQBENCH -p "/db/query" -n 15000000 -m 1000 "SELECT * FROM qux LIMIT 10" &

wait
