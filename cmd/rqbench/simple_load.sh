#!/bin/bash

if [[ -z "$EXECUTE_HOST" ]]; then
  EXECUTE_HOST=localhost:4001
fi

if [[ -z "$RQBENCH" ]]; then
  RQBENCH="./rqbench"
fi

handle_ctrl_c() {
    echo "Killing all load testing..."
    killall rqbench
    exit 1
}

COUNT=10000

trap 'handle_ctrl_c' SIGINT

$RQBENCH -o 'CREATE TABLE IF NOT EXISTS foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, surname TEXT)' -m 100 -n $COUNT -a $EXECUTE_HOST 'INSERT INTO foo(name) VALUES("fiona")' &
$RQBENCH -o 'CREATE TABLE IF NOT EXISTS bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, surname TEXT)' -m 100 -n $COUNT -a $EXECUTE_HOST 'INSERT INTO bar(name, surname) VALUES("fiona", "OTOOLE")' &
$RQBENCH -o 'CREATE TABLE IF NOT EXISTS qux (id INTEGER NOT NULL PRIMARY KEY, name TEXT)' -m 100 -n $COUNT -a $EXECUTE_HOST 'INSERT INTO qux(name) VALUES("fionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafionafiona")' &

echo "Waiting for tables to be created before starting queries"
sleep 5

$RQBENCH -p "/db/query" -n 150000000 -m 1000 "SELECT COUNT(*) FROM foo" &
$RQBENCH -p "/db/query" -n 150000000 -m 1000 "SELECT COUNT(*) FROM bar" &
$RQBENCH -p "/db/query" -n 150000000 -m 1000 "SELECT * FROM qux LIMIT 10" &

wait
