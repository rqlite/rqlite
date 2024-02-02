# Data API
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

Each rqlite node exposes an HTTP API allowing data to be inserted into, and read back from, the database. Any changes to the database  (`INSERT`, `UPDATE`, `DELETE`) **must** be sent to the `/db/execute` endpoint, and reads (`SELECT`) should be sent to the `/db/query` endpoint. _It is important to use the correct endpoint for the operation you wish to perform._

The best way to understand the API is to work through the simple examples below. There are also [client libraries available](https://github.com/rqlite).

## Writing Data
To write data successfully to the database, you must create at least 1 table. To do this perform a HTTP POST on the `/db/execute` endpoint on any rqlite node. Encapsulate the `CREATE TABLE` SQL command in a JSON array, and put it in the body of the request. An example via [curl](https://curl.haxx.se/):

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
Querying data is easy. For a single query simply perform an HTTP GET on the `/db/query` endpoint, setting the query statement as the query parameter `q`:

```bash
curl -G 'localhost:4001/db/query?pretty&timings' --data-urlencode 'q=SELECT * FROM foo'
```

The **default** response is of the form:

```json
{
    "results": [
        {
            "columns": [
                "id",
                "name",
                "age"
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

### Associative response form
A alternative form of response can be requested, by adding `associative` as a query parameter:
```bash
curl -G 'localhost:4001/db/query?pretty&timings&associative' --data-urlencode 'q=SELECT * FROM foo'
```
Response:
```json
{
    "results": [
        {
            "types": {"id": "integer", "age": "integer", "name": "text"},
            "rows": [
                { "id": 1, "age": 20, "name": "fiona"},
                { "id": 2, "age": 25, "name": "declan"}
            ],
            "time": 0.000173061
        }
    ],
    "time": 0.000185964
}
```
This form will have a map per row returned, with each column name as a key. This form can be more convenient for clients, depending on the application.

## Parameterized Statements
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

### Named Parameters
Named parameters are also supported. To use this feature set the values using a dictionary like so:

_Writing data_
```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name, age) VALUES(:name, :age)", {"name": "fiona", "age": 20}]
]'
```
_Reading data_
```bash
curl -XPOST 'localhost:4001/db/query?pretty&timings' -H "Content-Type: application/json" -d '[
    ["SELECT * FROM foo WHERE name=:name", {"name": "fiona"}]
]'
```


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
If an error occurs while processing a request, it will be indicated via the presence of an `error` key in the JSON response. For example:

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

## Queued Writes API
Queued Writes can provide an order-of-magnitude speed up in write-performance. You can learn about the Queued Writes API [here](https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md).

## Bulk API
You can learn about the Bulk write API [here](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md).

## How rqlite Handles Requests
_This section assumes a basic familiarity with the Raft protocol. A simple introduction to Raft can be found [here](http://thesecretlivesofdata.com/raft/)._

To make the very best use of the rqlite API, there are some important details to know. But understanding the following details is **not required** to make use of rqlite.

With any rqlite cluster, all write-requests must be serviced by the cluster Leader -- this is due to the way the Raft consensus protocol works. If a client sends a write request to a Follower (or read-only, non-voting, node), the Follower transparently forwards the request to the Leader. The Follower waits for the response from the Leader, and returns it to the client. Any credential information included in the original HTTP request to the Follower is included with the forwarded request (assuming that permission checking also passes first on the Follower), and permission checking is performed on the Leader.

Queries, by default, are also serviced by the cluster Leader. Like write-requests, Followers will, by default, transparently forward queries to the Leader, and respond to the client after receiving the response from the Leader. However, depending on the [read-consistency](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md) specified with the request, if a Follower received the query request it may serve that request directly and not contact the Leader. Which read-consistency level makes sense depends on your application.

### Data and the Raft log
Any writes to the SQLite database go through the Raft log, ensuring only changes committed by a quorum of rqlite nodes are actually applied to the SQLite database. Queries do not __necessarily__ go through the Raft log, however, since they do not change the state of the database, and therefore do not need to be captured in the log. Only if _Strong_ read consistency is requested does a query go through the Raft log.

### Request Forwarding Timeouts
If a Follower forwards a request to a Leader, by default the Leader must respond within 30 seconds. You can control this timeout by setting the `timeout` parameter. For example, to set a 2 minute timeout, you would issue the following request:
```bash
curl -XPOST 'localhost:4001/db/execute?timeout=2m' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20]
]'
```

### SQL Query Timeout
By default, SQL queries do not have a predefined timeout. You can set a timeout by sending a `db_timeout` query parameter in the request. This parameter allows you to specify the maximum amount of time to spend executing the query before it is interrupted.
```bash
curl -XPOST 'localhost:4001/db/execute?db_timeout=2s' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name, age) VALUES(?, ?)", "fiona", 20]
]'
```

### Disabling Request Forwarding
If you do not wish a Follower to transparently forward a request to a Leader, add `redirect` to the URL as a query parameter. In that case if a Follower receives a request that can only be serviced by the Leader, the Follower will respond with [HTTP 301 Moved Permanently](https://en.wikipedia.org/wiki/HTTP_301) and include the address of the Leader as the `Location` header in the response. It is then up the clients to re-issue the command to the Leader.

This option was made available as it provides maximum visibility to the clients, should they prefer if. For example, if a Follower transparently forwarded a request to the Leader, and one of the nodes then crashed during processing, it may be difficult for the client to determine where in the chain of nodes the processing failed.

### Example of redirect on query
```
$ curl -v -G 'localhost:4003/db/query?pretty&timings&redirect' --data-urlencode 'q=SELECT * FROM foo'
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


