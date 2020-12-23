This is a copy of a Raft directory formed using v5.6.0 software, after the following commands were issued:

curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)"
]'

curl -XPOST 'localhost:4001/db/execute?pretty&imings' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20]
]'
