# Data API

rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API. Modifications go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually executed against the SQLite database. Queries do not __necessarily__ go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log. More on this later.

There are also [client libraries available](https://github.com/rqlite).

## Writing Data
To write data successfully to the database, you must create at least 1 table. To do this perform a HTTP POST, with a `CREATE TABLE` SQL command encapsulated in a JSON array, in the body of the request. An example via [curl](http://curl.haxx.se/):

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "CREATE TABLE foo (id integer not null primary key, name text)"
]'
```

To insert an entry into the database, execute a second SQL command:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "INSERT INTO foo(name) VALUES(\"fiona\")"
]'
```

The response is of the form:

```json
{
    "results": [
        {
            "last_insert_id": 1,
            "rows_affected": 1,
            "time": 0.00886
        }
    ],
    "time": 0.0152
}
```

The use of the URL param `pretty` is optional, and results in pretty-printed JSON responses. Time is measured in seconds. If you do not want timings, do not pass `timings` as a URL parameter.

## Querying Data
Querying data is easy. The most important thing to know is that, by default, queries must go through the leader node. More on this later.

For a single query simply perform a HTTP GET, setting the query statement as the query parameter `q`:

```bash
curl -G 'localhost:4001/db/query?pretty&timings' --data-urlencode 'q=SELECT * FROM foo'
```

The response is of the form:

```json
{
    "results": [
        {
            "columns": [
                "id",
                "name"
            ],
            "types": [
                "integer",
                "text"
            ],
            "values": [
                [
                    1,
                    "fiona"
                ]
            ],
            "time": 0.0150043
        }
    ],
    "time": 0.0220043
}
```

### Read Consistency
You can learn all about the read consistency guarantees supported by rqlite [here](https://github.com/rqlite/rqlite/blob/master/doc/CONSISTENCY.md).

## Transactions
Transactions are supported. To execute statements within a transaction, add `transaction` to the URL. An example of the above operation executed within a transaction is shown below.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&transaction' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
```

When a transaction takes place either both statements will succeed, or neither. Performance is *much, much* better if multiple SQL INSERTs or UPDATEs are executed via a transaction. Note that processing of the request ceases the moment any single query results in an error.

The behaviour of rqlite when using `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` to control transactions is **not defined**. It is important to control transactions only through the query parameters shown above.

## Handling Errors
If an error occurs while processing a statement, it will be marked as such in the response. For example.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d "[
    \"INSERT INTO nonsense\"
]"
```
```json
{
    "results": [
        {
            "error": "near \"nonsense\": syntax error"
        }
    ],
    "time": 2.478862
}
```
## Bulk API
You can learn about the bulk API [here](https://github.com/rqlite/rqlite/blob/master/doc/BULK.md).


