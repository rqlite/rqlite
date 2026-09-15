#!/bin/bash

curl -s -XPOST 'localhost:4001/db/execute' -H "Content-Type: application/json" -d '[
    "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"
]'

for i in {1..20}
do
   curl -s -XPOST 'localhost:4001/db/execute' -H "Content-Type: application/json" -d '[
    "INSERT INTO foo(name) VALUES(\"fiona\")"
]'
done
