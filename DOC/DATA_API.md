# Data API

rqlite exposes an HTTP API allowing the database to be modified such that the changes are replicated. Queries are also executed using the HTTP API. _It is important to use the correct endpoint for the operation you wish to execute._

All write-requests must be sent to the leader of the cluster. Queries, however, may be sent to any node, depending on the [read-consistency](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md) requirements. But, by default, queries must also be sent to the leader.

There are [client libraries available](https://github.com/rqlite).

## Data and the Raft log
Any modifications to the SQLite database go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually executed against the SQLite database. Queries do not __necessarily__ go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log. More on this later.

## Writing Data
To write data successfully to the database, you must create at least 1 table. To do this perform a HTTP POST on the `/execute` endpoint. Encapsulate the `CREATE TABLE` SQL command in a JSON array, and put it in the body of the request. An example via [curl](http://curl.haxx.se/):

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)"
]'
```

To insert an entry into the database, execute a second SQL command:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    "INSERT INTO foo(name, age) VALUES(\"fiona\", 20)"
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
Querying data is easy. The most important thing to know is that, by default, queries must go through the leader node. 

For a single query simply perform a HTTP GET on the `/query` endpoint, setting the query statement as the query parameter `q`:

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
                "text",
                "integer"
            ],
            "values": [
                [
                    1,
                    "fiona",
                    20
                ]
            ],
            "time": 0.0150043
        }
    ],
    "time": 0.0220043
}
```

You can also query via a HTTP POST request:
```bash
curl -XPOST 'localhost:4001/db/query?pretty&timings' -H "Content-Type: application/json" -d '[
    "SELECT * FROM foo"
]'
```
The response will be in the same form as when the query is made via HTTP GET.

### Read Consistency
You can learn all about the read consistency guarantees supported by rqlite [here](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md).

## Parameterized Statements
_Support for Parameterized Statements was introduced in v5.5.0. The commands below will not work with earlier versions of rqlite._

While the "raw" API described above can be convenient and simple to use, it is vulnerable to [SQL Injection attacks](https://owasp.org/www-community/attacks/SQL_Injection). To protect against this issue, rqlite also supports [SQLite parameterized statements](https://www.sqlite.org/lang_expr.html#varparam), for both read and writes. To use this feature, send the SQL statement and values as distinct elements within a new JSON array, as follows:

_Writing data_
```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20]
]'
```
_Reading data_
```bash
curl -XPOST 'localhost:4001/db/query?pretty&timings' -H "Content-Type: application/json" -d '[
    ["SELECT * FROM foo WHERE name=?", "fiona"]
]'
```
Currently named parameters are not yet supported, only simple parameters that use `?`. The API also does not support mixing the parameterized and non-parameterized form in a single request.

## Transactions
A **form** of transactions are supported. To execute statements within a transaction, add `transaction` to the URL. An example of the above operation executed within a transaction is shown below.

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&transaction' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
```

When a transaction takes place either both statements will succeed, or neither. Performance is *much, much* better if multiple SQL INSERTs or UPDATEs are executed via a transaction. Note that processing of the request ceases the moment any single query results in an error.

The behaviour of rqlite if you explicitly issue `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, and `RELEASE` to control your own transactions is **not defined**. This is because the behavior of a cluster if it fails while such a manually-controlled transaction is not yet defined. It is important to control transactions only through the query parameters shown above.

## Handling Errors
If an error occurs while processing a statement, it will be marked as such in the response. For example:

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

## Sending requests to followers
What happens when you send a request to a follower depends on the nature of the request.

You must always send write-requests (requests will change the database) to the leader. If you send a write-request to a follower, the follower will respond with [HTTP 301 Moved Permanently](https://en.wikipedia.org/wiki/HTTP_301) and include the address of the leader as the `Location` header in the response.

The situation for queries -- requests which just read data -- is somewhat different. If you send the request to a node that is not the leader of the cluster, and specify `strong` or `weak` as the [read-consistency level](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md), the node will also respond with [HTTP 301 Moved Permanently](https://en.wikipedia.org/wiki/HTTP_301) and include the address of the leader as the `Location` header in the response.

However, if you specify `none` for read-consistency the node will query its local SQLite database. No redirect will be returned.

## Example of redirect on query
```
$ curl -v -G 'localhost:4003/db/query?pretty&timings' --data-urlencode 'q=SELECT * FROM foo'
*   Trying ::1...
* connect to ::1 port 4003 failed: Connection refused
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 4003 (#0)
> GET /db/query?pretty&timings&q=SELECT%20%2A%20FROM%20foo HTTP/1.1
> Host: localhost:4003
> User-Agent: curl/7.43.0
> Accept: */*
> 
< HTTP/1.1 301 Moved Permanently
< Content-Type: application/json; charset=utf-8
< Location: http://localhost:4001/db/query?pretty&timings&q=SELECT%20%2A%20FROM%20foo
< X-Rqlite-Version: 4
< Date: Mon, 07 Aug 2017 21:10:59 GMT
< Content-Length: 116
< 
<a href="http://localhost:4001/db/query?pretty&amp;timings&amp;q=SELECT%20%2A%20FROM%20foo">Moved Permanently</a>.

* Connection #0 to host localhost left intact
```
It is up the clients to re-issue the command to the leader.

This choice was made as it provides maximum visibility to the clients. For example, if a follower transparently forwarded a request to the leader, and one of the nodes then crashed during processing, it may be much harder for the client to determine where in the chain of nodes the processing failed.

## Bulk API
You can learn about the bulk API [here](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md).


